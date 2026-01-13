import pandas as pd
import numpy as np
from config import project_csv, logistics_csv, inventory_csv, fix_cols, read_csv

prev1_project_csv = "project--2025-12-31.csv"
prev1_logistics_csv = "logistics--2025-12-31.csv"
prev1_inventory_csv = "inventory--2025-12-31.csv"

prev2_project_csv = "project--2025-11-30.csv"
prev2_logistics_csv = "logistics--2025-11-30.csv"
prev2_inventory_csv = "inventory--2025-11-30.csv"

prev3_project_csv = "project--2025-10-31.csv"
prev3_logistics_csv = "logistics--2025-10-31.csv"
prev3_inventory_csv = "inventory--2025-10-31.csv"


# 리드타임 계산 함수
def calculate_task_leadtime(
    project_data: str,
    logistics_data: str,
    inventory_data: str
) -> tuple[pd.DataFrame, pd.DataFrame]:

    project = read_csv(project_data, fix_cols["project"], "project")
    logistics = read_csv(logistics_data, fix_cols["logistics"], "logistics")
    inventory = read_csv(inventory_data, fix_cols["inventory"], "inventory")

    # 필요 컬럼 추출
    log_small = logistics[["date", "logistics_id", "project_id", "logistic_create_at", "logistics_status", "logistics_completed_at"]].copy()
    inv_small = inventory[["date", "inventory_id", "project_id", "inventory_create_at", "inventory_status", "inventory_completed_at"]].copy()
    proj_small = project[["project_id", "company_id"]].copy()

    # 데이터 타입 정리
    log_small["logistic_create_at"] = pd.to_datetime(log_small["logistic_create_at"], errors="coerce")
    log_small["logistics_completed_at"] = pd.to_datetime(log_small["logistics_completed_at"], errors="coerce")
    log_small["logistics_status"] = log_small["logistics_status"].astype(str)

    inv_small["inventory_create_at"] = pd.to_datetime(inv_small["inventory_create_at"], errors="coerce")
    inv_small["inventory_completed_at"] = pd.to_datetime(inv_small["inventory_completed_at"], errors="coerce")
    inv_small["inventory_status"] = inv_small["inventory_status"].astype(str)

    # 날짜 타입 변환 -> 기존 max()에서 datetime으로 바로 변환하는 걸로 로직 변경
    log_small["date"] = pd.to_datetime(log_small["date"], errors="coerce")
    inv_small["date"] = pd.to_datetime(inv_small["date"], errors="coerce")
    month = str(log_small["date"].max().to_period("M"))

    log_small["_end_month"] = log_small["logistics_completed_at"].dt.to_period("M").astype(str)
    inv_small["_end_month"] = inv_small["inventory_completed_at"].dt.to_period("M").astype(str)

    # 데이터 병합 (LEFT JOIN)
    log_df = (
        log_small
        .merge(proj_small, on="project_id", how="left")
    )

    inv_df = (
        inv_small
        .merge(proj_small, on="project_id", how="left")
    )

    # 업무별 리드타임 계산
    log_mask = (log_df["logistics_status"] == "완료") & (log_df["logistic_create_at"].notna()) & (log_df["logistics_completed_at"].notna()) & ((log_df["_end_month"] == month))

    log_df.loc[log_mask, "logistics_lead_time"] = (
        (log_df.loc[log_mask, "logistics_completed_at"] - log_df.loc[log_mask, "logistic_create_at"])
        .dt.total_seconds()
        / 3600.0
    )

    inv_mask = (inv_df["inventory_status"] == "완료") & (inv_df["inventory_create_at"].notna()) & (inv_df["inventory_completed_at"].notna())& ((inv_df["_end_month"] == month))

    inv_df.loc[inv_mask, "inventory_lead_time"] = (
        (inv_df.loc[inv_mask, "inventory_completed_at"] - inv_df.loc[inv_mask, "inventory_create_at"])
        .dt.total_seconds()
        / 3600.0
    )

    # 필요 데이터만 추출
    log = log_df.loc[log_mask, ["date", "company_id", "project_id", "logistics_id", "logistics_lead_time"]]
    inv = inv_df.loc[inv_mask, ["date", "company_id", "project_id", "inventory_id", "inventory_lead_time"]]

    return log, inv
    

#SLA 계산 함수
def calculate_sla (
    prev_logs: list[pd.DataFrame],
    prev_invs: list[pd.DataFrame]
) -> pd.DataFrame:
    
    # 3개월 데이터 병합
    logs = pd.concat(prev_logs, ignore_index=True)

    invs = pd.concat(prev_invs, ignore_index=True)

    # 데이터 전처리
    logs = logs[
        logs["logistics_lead_time"].notna()
        & (logs["logistics_lead_time"] > 0)
    ]

    invs = invs[
        invs["inventory_lead_time"].notna()
        & (invs["inventory_lead_time"] > 0)
    ]

    # 회사별 SLA 기준 계산
    logs = logs.groupby("company_id")["logistics_lead_time"]
    invs = invs.groupby("company_id")["inventory_lead_time"]

    log_sla = pd.DataFrame({
        "company_id": logs.size().index,
        "log_sla_size": logs.size().values,
        "log_sla_p80": logs.quantile(0.8).values,
    })

    inv_sla = pd.DataFrame({
        "company_id": invs.size().index,
        "inv_sla_size": invs.size().values,
        "inv_sla_p80": invs.quantile(0.8).values,
    })

    sla_df = log_sla.merge(inv_sla, on="company_id", how="outer")

    return sla_df
    

# 업무 장기 처리율 계산 함수
def long_term_task_rate() -> pd.DataFrame:
    
    cur_log, cur_inv = calculate_task_leadtime (
        project_data=project_csv,
        logistics_data=logistics_csv,
        inventory_data=inventory_csv
    )

    # 날짜 변환
    as_of_date = pd.to_datetime(cur_log["date"], errors="coerce").max()
    month = str(as_of_date.to_period("M"))

    # SLA 기준 계산 (3개월)
    # 3개월 데이터 로드
    prev1_log, prev1_inv = calculate_task_leadtime (
        project_data=prev1_project_csv,
        logistics_data=prev1_logistics_csv,
        inventory_data=prev1_inventory_csv
    )

    prev2_log, prev2_inv = calculate_task_leadtime (
        project_data=prev2_project_csv,
        logistics_data=prev2_logistics_csv,
        inventory_data=prev2_inventory_csv
    )

    prev3_log, prev3_inv = calculate_task_leadtime (
        project_data=prev3_project_csv,
        logistics_data=prev3_logistics_csv,
        inventory_data=prev3_inventory_csv
    )

    # company 별 SLA 계산
    sla_df = calculate_sla(
        prev_logs=[prev1_log, prev2_log, prev3_log],
        prev_invs=[prev1_inv, prev2_inv, prev3_inv]
    )

    # 장기 업무 계산 (> SLA)
    cur_log = cur_log.merge(
        sla_df[["company_id", "log_sla_p80"]],
        on="company_id",
        how="left"
    )

    cur_log["over_sla"] = np.where(
        cur_log["log_sla_p80"].notna(),
        cur_log["logistics_lead_time"] > cur_log["log_sla_p80"],
        np.nan
    )

    cur_inv = cur_inv.merge(
            sla_df[["company_id", "inv_sla_p80"]],
            on="company_id",
            how="left"
        )

    cur_inv["over_sla"] = np.where(
        cur_inv["inv_sla_p80"].notna(),
        cur_inv["inventory_lead_time"] > cur_inv["inv_sla_p80"],
        np.nan
    )
    
    log_kpi = (
        cur_log.groupby("company_id", as_index=False)
        .agg(
            logistics_total=("logistics_id", "count"),
            logistics_over_sla=("over_sla", "sum")
        )
    )

    inv_kpi = (
        cur_inv.groupby("company_id", as_index=False)
        .agg(
            inventory_total=("inventory_id", "count"),
            inventory_over_sla=("over_sla", "sum")
        )
    )

    # 출하, 재고 kpi 병합해서 최종 kpi 추출
    kpi = log_kpi.merge(inv_kpi, on="company_id", how="outer")

    # 출하, 입고만 있는 경우 대비 0으로 채우기
    for c in ["logistics_total", "logistics_over_sla", "inventory_total", "inventory_over_sla"]:
        kpi[c] = kpi[c].fillna(0).astype(int)

    kpi["total_tasks"] = kpi["logistics_total"] + kpi["inventory_total"]
    kpi["total_over_sla"] = kpi["logistics_over_sla"] + kpi["inventory_over_sla"]

    kpi["long_term_task_rate"] = np.where(
        kpi["total_tasks"] > 0,
        (kpi["total_over_sla"] / kpi["total_tasks"] * 100).round(3),
        np.nan
    )

    kpi["month"] = month

    return kpi[["month", "company_id", "long_term_task_rate"]]