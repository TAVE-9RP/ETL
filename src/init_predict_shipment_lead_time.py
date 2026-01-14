import pandas as pd
import numpy as np
import xgboost as xgb
import warnings

warnings.filterwarnings('ignore')

path = "./data/json_leadtime_mock.csv"

# 데이터셋 로드 -> etl 레포에서 데이터 로드 따로 해서 빼둠
def load_leadtime_csv() -> pd.DataFrame:

    df = pd.read_csv(path)

    df["companyId"] = pd.to_numeric(df["companyId"], errors="coerce")
    df["snapshotDate"] = pd.to_datetime(df["snapshotDate"], errors="coerce")
    df["shipmentLeadTimeAvg"] = pd.to_numeric(df["shipmentLeadTimeAvg"], errors="coerce")
    df = df.dropna(subset=["companyId", "snapshotDate"]).copy()

    # 월말 검증 -> 월말이 아닌 값은 제거
    month_end = df["snapshotDate"] + pd.offsets.MonthEnd(0)
    df = df[df["snapshotDate"] == month_end].copy()

    # 중복 제거
    df = df.sort_values("snapshotDate")
    df = df.drop_duplicates(subset=["companyId", "snapshotDate"], keep="last")

    return df.sort_values(["companyId", "snapshotDate"]).reset_index(drop=True)


# 외생변수 없는 시계열용 피처 생성 함수 -> 시계열을 회귀로 바꾸는 작업 (XGB용)
def make_features(
    df: pd.DataFrame,
    target_col: str,
    lags=(1, 2, 3, 6),
    rolls=(3, 6),
) -> pd.DataFrame:
    
    out = df.copy()
    out = out.sort_values("snapshotDate").reset_index(drop=True)

    # month-of-year (1~12)
    out["moy"] = out["snapshotDate"].dt.month
    # 계절을 더 부드럽게 먹이기 위한 사인/코사인
    out["moy_sin"] = np.sin(2 * np.pi * out["moy"] / 12)
    out["moy_cos"] = np.cos(2 * np.pi * out["moy"] / 12)

    for k in lags:
        out[f"lag_{k}"] = out[target_col].shift(k)
    
    # rolling stats (shift(1)로 미래 누수 방지 -> t rolling 계산할 때 t-1까지의 값만 사용하도록)
    for w in rolls:
        s = out[target_col].shift(1)
        out[f"roll_mean_{w}"] = s.rolling(w, min_periods=1).mean()
        out[f"roll_std_{w}"] = s.rolling(w, min_periods=2).std()

    return out

    
def forecast_xgb(
    target_col = "shipmentLeadTimeAvg",
    H: int = 1,
    min_history: int = 24,
) -> pd.DataFrame:

    df = load_leadtime_csv()

    # 날짜 변환 ('YYYY-MM' -> datetime)
    df['ds'] = pd.to_datetime(df['snapshotDate'], format='%Y-%m', errors='coerce')
    
    # NaN 데이터 제거 (날짜 변환 실패 또는 리드타임 없는 경우)
    df = df.dropna(subset=['ds', 'shipmentLeadTimeAvg'])
    
    # 회사별 XGBoost 예측 루프
    predicts = []
    unique_companies = df['companyId'].unique()

    for cid, g in df.groupby("companyId"):
        # 해당 회사의 데이터 추출 및 날짜순 정렬
        g = g.sort_values('ds').reset_index(drop=True).copy()
        
        # 데이터가 너무 적으면(예: 3개월 미만) 학습 불가로 판단하여 스킵
        if len(g) < min_history:
            lack = min_history - len(g)
            print(f"companyId={cid}: 최근 {min_history}개월 기준 {lack}개월 부족해서 예측을 돌릴 수 없습니다.")
            continue

        # 학습 피쳐 생성
        feat = make_features(g[["snapshotDate", target_col]].copy(), target_col=target_col)
        feature_cols = [c for c in feat.columns if c not in ["snapshotDate", target_col, "companyId"]]

        train_mask = feat[feature_cols].notnull().all(axis=1)
        X_train = feat.loc[train_mask, feature_cols]
        y_train = feat.loc[train_mask, target_col].astype(float)
        
        # 모델 초기화 및 학습
        model = xgb.XGBRegressor(
            n_estimators=500,
            learning_rate=0.05,
            max_depth=5,
            random_state=42,
            n_jobs=-1
        )
        model.fit(X_train, y_train)
        
        # 지금 월말 -> 다음달 월말
        hist_plus = g[["snapshotDate", target_col]].copy()
        last_date = hist_plus["snapshotDate"].max()

        # 익월 재고 회전율 예측
        for h in range(1, H + 1):
            # 월말 기준 -> 다음달 월말
            next_date = last_date + pd.offsets.MonthEnd(1)

            tmp = pd.concat(
            [hist_plus, pd.DataFrame({"snapshotDate": [next_date], target_col: [np.nan]})],
            ignore_index=True,
            )
            feat_tmp = make_features(tmp, target_col=target_col)
            X_last = feat_tmp.iloc[-1:][feature_cols]

            y_hat = float(model.predict(X_last)[0])

            predicts.append({
                "companyId": cid,
                "snapshotDate": next_date,
                f"{target_col}_pred": round(y_hat, 3),
                "xgb_used": "XGBoost"
            })

            # 예측값 저장
            hist_plus = pd.concat(
                [hist_plus, pd.DataFrame({"snapshotDate": [next_date], target_col: [y_hat]})],
                ignore_index=True,
            )
            last_date = next_date

    kpi =  pd.DataFrame(predicts)
    return kpi

if __name__ == "__main__":
    forecast_df = forecast_xgb(H=1, min_history=24)

    # json으로 저장할 정보
    forecast_kpi = forecast_df[["companyId", "snapshotDate", "shipmentLeadTimeAvg_pred"]]

    print(forecast_df)