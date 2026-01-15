import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

from statsmodels.tsa.holtwinters import ExponentialSmoothing
from xgboost import XGBRegressor

path = "./data/json_turnover_mock.csv"


# 데이터셋 로드 -> etl 레포에서 데이터 로드 따로 해서 빼둠
def load_turnover_csv() -> pd.DataFrame:

    df = pd.read_csv(path)

    df["companyId"] = pd.to_numeric(df["companyId"], errors="coerce")
    df["snapshotDate"] = pd.to_datetime(df["snapshotDate"], errors="coerce")
    df["turnOverRate"] = pd.to_numeric(df["turnOverRate"], errors="coerce")
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


# exponential Smoothing 모델 적합 함수
def fit_ets(y: pd.Series):
    y = pd.Series(y).astype(float)

    # 데이터가 2년 미만이면 계절성이 오히려 방해 -> 2년 이상 축적되었을 때부터 예측 코드 작동하게끔
    seasonal_periods = 12 if len(y) >= 24 else None
    seasonal = "add" if seasonal_periods is not None else None

    model = ExponentialSmoothing(
        y,
        trend="add",           
        damped_trend=True,      
        seasonal=seasonal,      
        seasonal_periods=seasonal_periods,
        initialization_method="estimated",
    )
    res = model.fit(optimized=True)
    return res


# XGBoost 회귀 모델 적합 함수 -> ETS가 못 맞춘 잔차를 XGB가 학습하는 로직
def fit_xgb_residual(X: pd.DataFrame, y_resid: pd.Series) -> XGBRegressor:
    model = XGBRegressor(
        n_estimators=600,
        learning_rate=0.03,
        max_depth=4,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_lambda=1.0,
        reg_alpha=0.0,
        objective="reg:squarederror",
        random_state=42,
    )
    model.fit(X, y_resid)
    return model


# 시계열 예측 함수 (ETS + XGB 잔차 보정)
def forecast_ets_xgb(
    df_company: pd.DataFrame,
    target_col: str = "turnOverRate",
    H: int = 1,
    min_history: int = 24,
    min_xgb_rows: int = 12,
):
    # 날짜 정렬 
    df = df_company.sort_values("snapshotDate").reset_index(drop=True).copy()

    # 재고 회전율 값이면 nan이면 drop -> NAN 있으면 ETS 이상해짐
    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")
    df[target_col] = df[target_col].replace([np.inf, -np.inf], np.nan).clip(lower=0)
    df = df.dropna(subset=[target_col]).copy()

    y = df[target_col].astype(float).reset_index(drop=True)

    # 2년(24개) 데이터를 추출할 수 있는 경우에만 예측 진행
    if len(y) < min_history:
        return pd.DataFrame()

    # ETS
    ets_res = fit_ets(y)
    ets_fc = ets_res.forecast(H).astype(float).reset_index(drop=True)

    # 잔차 계산
    fitted = pd.Series(ets_res.fittedvalues).astype(float).reset_index(drop=True)
    resid = y - fitted

    df2 = df.copy()
    df2[target_col] = y.values

    # XGB 잔차 피처
    feat_hist = make_features(df2, target_col=target_col)
    feature_cols = [c for c in feat_hist.columns if c not in [ "companyId", "snapshotDate", target_col]]

    # XGB 학습
    train_mask = feat_hist[feature_cols].notnull().all(axis=1)
    X_train = feat_hist.loc[train_mask, feature_cols]
    y_resid_train = resid.loc[train_mask]

    use_xgb = len(X_train) >= min_xgb_rows
    xgb = fit_xgb_residual(X_train, y_resid_train) if use_xgb else None

    predicts = []
    hist_plus = df2[["snapshotDate", target_col]].copy().reset_index(drop=True)
    last_date = hist_plus["snapshotDate"].max()

    # 익월 재고 회전율 예측
    for h in range(1, H + 1):
        # 월말 기준 -> 다음달 월말
        next_date = last_date + pd.offsets.MonthEnd(1)

        ets_hat = float(ets_fc.iloc[h - 1])

        tmp = pd.concat(
            [hist_plus, pd.DataFrame({"snapshotDate": [next_date], target_col: [np.nan]})],
            ignore_index=True,
        )
        feat_tmp = make_features(tmp, target_col=target_col)
        X_last = feat_tmp.iloc[-1:][feature_cols]

        resid_hat = float(xgb.predict(X_last)[0]) if use_xgb else 0.0
        y_hat = max(0.0, ets_hat + resid_hat)

        predicts.append({
            "snapshotDate": next_date,
            f"{target_col}_ets": round(ets_hat, 3),
            f"{target_col}_resid": round(resid_hat, 3),
            f"{target_col}_pred": round(y_hat, 3),
            "xgb_used": use_xgb,
        })

        hist_plus = pd.concat(
            [hist_plus, pd.DataFrame({"snapshotDate": [next_date], target_col: [y_hat]})],
            ignore_index=True,
        )
        last_date = next_date

    return pd.DataFrame(predicts)


# 회사별로 예측 진행 함수
def forecast_all_companies(
    turnover_predict: pd.DataFrame,
    H: int = 1,
    min_history: int = 24,
) -> pd.DataFrame:

    out = []

    for cid, g in turnover_predict.groupby("companyId"):
        g = g.sort_values("snapshotDate").reset_index(drop=True)

        if len(g) < min_history:
            lack = min_history - len(g)
            print(f"companyId = {cid}: 최근 {min_history}개월 기준 {lack}개월 부족해서 예측을 돌릴 수 없습니다.")
            continue

        fc = forecast_ets_xgb(g, target_col="turnOverRate", H=H)
        if len(fc) == 0:
            continue

        fc.insert(0, "companyId", cid)
        out.append(fc)

    kpi = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    
    return kpi


# mock 데이터로 결과 확인
if __name__ == "__main__":
    df = load_turnover_csv()
    forecast_df = forecast_all_companies(df)

    # json으로 저장할 정보
    forecast_kpi = forecast_df[["companyId", "snapshotDate", "turnOverRate_pred"]]

    print(forecast_df)