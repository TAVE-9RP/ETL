import pandas as pd
import numpy as np
import xgboost as xgb

def _make_features(df: pd.DataFrame, target_col: str, lags=(1, 2, 3, 6), rolls=(3, 6)):
    """시계열 피처 생성"""
    out = df.copy()
    out = out.sort_values("snapshotDate").reset_index(drop=True)
    out["moy"] = out["snapshotDate"].dt.month
    out["moy_sin"] = np.sin(2 * np.pi * out["moy"] / 12)
    out["moy_cos"] = np.cos(2 * np.pi * out["moy"] / 12)

    for k in lags:
        out[f"lag_{k}"] = out[target_col].shift(k)
    for w in rolls:
        s = out[target_col].shift(1)
        out[f"roll_mean_{w}"] = s.rolling(w, min_periods=1).mean()
        out[f"roll_std_{w}"] = s.rolling(w, min_periods=2).std()
    return out

def forecast_lead_time_xgb(df_source: pd.DataFrame, target_col="shipmentLeadTimeAvg", H=1, min_history=24):
    """
    S3에서 로드된 DF를 받아 익월 리드타임을 예측하여 리스트로 반환
    """
    if df_source.empty:
        return []

    df = df_source.copy()
    df["snapshotDate"] = pd.to_datetime(df["snapshotDate"])
    df["companyId"] = pd.to_numeric(df["companyId"], errors="coerce")
    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")


    # 월말 데이터 정규화 및 중복 제거
    month_end = df["snapshotDate"] + pd.offsets.MonthEnd(0)
    df = df[df["snapshotDate"] == month_end].copy()
    df = df.sort_values(["companyId", "snapshotDate"]).drop_duplicates(subset=["companyId", "snapshotDate"], keep="last")

    results = []
    for cid, g in df.groupby("companyId"):
        g = g.sort_values("snapshotDate").reset_index(drop=True).copy()

        if len(g) < min_history:
            print(f"[Skip] Company {cid} has only {len(g)} months.")
            continue

        feat = _make_features(g[["snapshotDate", target_col]].copy(), target_col=target_col)
        feature_cols = [c for c in feat.columns if c not in ["snapshotDate", target_col, "companyId"]]

        train_mask = feat[feature_cols].notnull().all(axis=1)
        X_train = feat.loc[train_mask, feature_cols]
        y_train = feat.loc[train_mask, target_col].astype(float)

        if X_train.empty:
            continue

        model = xgb.XGBRegressor(n_estimators=500, learning_rate=0.05, max_depth=5, random_state=42, n_jobs=-1)
        model.fit(X_train, y_train)

        # 예측 (익월 말일 기준)
        last_date = g["snapshotDate"].max()
        next_date = last_date + pd.offsets.MonthEnd(1)

        tmp = pd.concat([g[["snapshotDate", target_col]],
                         pd.DataFrame({"snapshotDate": [next_date], target_col: [np.nan]})], ignore_index=True)
        feat_tmp = _make_features(tmp, target_col=target_col)
        X_last = feat_tmp.iloc[-1:][feature_cols]

        y_hat = float(model.predict(X_last)[0])

        results.append({
            "company_id": int(cid),
            "pred_shipment_lead_time": round(y_hat, 3)
        })

    return results