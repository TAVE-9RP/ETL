import pandas as pd
import numpy as np
from config import project_csv, logistics_csv, fix_cols, read_csv

# 출하 리드타임 계산 함수
def shipment_lead_time_monthly() -> pd.DataFrame:
    
    project = read_csv(project_csv, fix_cols["project"], "project")
    logistics = read_csv(logistics_csv, fix_cols["logistics"], "logistics")

    # 필요 컬럼 추출
    log_small = logistics[["date", "logistics_id", "project_id", "logistic_create_at", "logistics_status", "logistics_completed_at"]].copy()
    proj_small = project[["date", "project_id", "company_id"]].copy()

    # 데이터 타입 정리
    log_small["logistic_create_at"] = pd.to_datetime(log_small["logistic_create_at"], errors="coerce")
    log_small["logistics_completed_at"] = pd.to_datetime(log_small["logistics_completed_at"], errors="coerce")
    log_small["logistics_status"] = log_small["logistics_status"].astype(str)

    log_small["date"] = pd.to_datetime(log_small["date"], errors="coerce")
    month = str(df["date"].max().to_period("M"))

    log_small["_end_month"] = log_small["logistics_completed_at"].dt.to_period("M").astype(str)

    # 데이터 병합 (LEFT JOIN)
    df = (
        log_small
        .merge(proj_small, on="project_id", how="left")
    )

    # 리드타임 계산 (hour 단위)
    mask_done = (df["logistics_status"] == "완료") & (df["logistic_create_at"].notna()) & (df["logistics_completed_at"].notna()) & ((df["_end_month"] == month))

    df.loc[mask_done, "lead_time_hours"] = (
        (df.loc[mask_done, "logistics_completed_at"] - df.loc[mask_done, "logistic_create_at"])
        .dt.total_seconds()
        / 3600.0
    )

    # company 별 리드타임 집계
    kpi = (
        df.loc[mask_done]
        .groupby("company_id", as_index=False)
        .agg(
            shipment_lead_time=("lead_time_hours", "mean")
        )
    )

    kpi["month"] = month

    return kpi[["month","company_id", "shipment_lead_time"]]