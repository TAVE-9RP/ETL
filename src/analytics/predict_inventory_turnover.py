import pandas as pd
import numpy as np
import xgboost as xgb
from statsmodels.tsa.holtwinters import ExponentialSmoothing

def _make_features(df: pd.DataFrame, target_col: str, lags=(1, 2, 3, 6), rolls=(3, 6)):
    """시계열 피처 생성 (내부용)"""
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

def forecast_inventory_turnover_hybrid(df_source: pd.DataFrame, target_col="turnOverRate", H=1, min_history=12):
    """
    ETS + XGBoost 하이브리드 예측
    익월 재고 회전율 예측 결과를 리스트로 반환
    """

    if df_source.empty:
        return []

    df = df_source.copy()
    df["snapshotDate"] = pd.to_datetime(df["snapshotDate"], errors='coerce')
    df["companyId"] = pd.to_numeric(df["companyId"], errors="coerce")
    df[target_col] = pd.to_numeric(df[target_col], errors="coerce").clip(lower=0)
    df = df.dropna(subset=["snapshotDate", "companyId", target_col])

    # 월말 정규화
    df["snapshotDate"] = df["snapshotDate"] + pd.offsets.MonthEnd(0)
    df = df.sort_values(["companyId", "snapshotDate"]).drop_duplicates(subset=["companyId", "snapshotDate"], keep="last")

    results = []
    for cid, g in df.groupby("companyId"):
        g = g.sort_values("snapshotDate").reset_index(drop=True)
        y = g[target_col].astype(float)

        if len(y) < min_history:
            continue

        try:
            # 1. ETS 적합 및 예측
            seasonal_periods = 12 if len(y) >= 24 else None
            seasonal = "add" if seasonal_periods else None

            ets_model = ExponentialSmoothing(
                y,
                trend="add",
                damped_trend=True,
                seasonal=seasonal,
                seasonal_periods=seasonal_periods,
                initialization_method="estimated"
            )
            ets_res = ets_model.fit(optimized=True)
            ets_fc = float(ets_res.forecast(H).iloc[0])

            # 2. XGBoost를 이용한 잔차(Residual) 보정
            fitted = pd.Series(ets_res.fittedvalues).astype(float)
            resid = y - fitted

            feat_hist = _make_features(g[["snapshotDate", target_col]], target_col=target_col)
            feature_cols = [c for c in feat_hist.columns if c not in ["snapshotDate", target_col]]

            train_mask = feat_hist[feature_cols].notnull().all(axis=1)
            X_train = feat_hist.loc[train_mask, feature_cols]
            y_resid_train = resid.loc[train_mask]

            resid_hat = 0.0
            if len(X_train) >= 12: # 최소 학습 데이터 확보 시
                xgb_model = xgb.XGBRegressor(
                    n_estimators=600,
                    learning_rate=0.03,
                    max_depth=4,
                    subsample=0.9,
                    colsample_bytree=0.9,
                    reg_lambda=1.0,
                    objective="reg:squarederror",
                    random_state=42)
                xgb_model.fit(X_train, y_resid_train)

                # 익월 피처 생성 및 예측
                next_date = g["snapshotDate"].max() + pd.offsets.MonthEnd(1)
                tmp = pd.concat([g[["snapshotDate", target_col]],
                                 pd.DataFrame({"snapshotDate": [next_date], target_col: [np.nan]})], ignore_index=True)
                feat_next = _make_features(tmp, target_col=target_col)
                X_next = feat_next.iloc[-1:][feature_cols]
                resid_hat = float(xgb_model.predict(X_next)[0])

            # ETS 결과와 XGB 잔차 예측값 합산 (최솟값 0 보정)
            y_hat = max(0.0, ets_fc + resid_hat)

            results.append({
                "company_id": int(cid),
                "pred_inventory_turnover": round(y_hat, 3)
            })
        except Exception as e:
            print(f"[Error] Hybrid model failed for Company {cid}: {e}")
            continue

    return results