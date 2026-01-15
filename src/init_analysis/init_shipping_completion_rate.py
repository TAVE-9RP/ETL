import pandas as pd
import numpy as np
from config import project_csv, logistics_csv, fix_cols, read_csv

# 이전월 출하 업무 스냅샷 파일 경로
prev_month_csv = "logistics--2025-12-31.csv"

# 월말 출하 완료율 계산 함수
def shipping_completion_rate_monthly() -> pd.DataFrame:

    project = read_csv(project_csv, fix_cols["project"], "project")
    logistics = read_csv(logistics_csv, fix_cols["logistics"], "logistics")
    prev_logistics = read_csv(prev_month_csv, fix_cols["logistics"], "prev_logistics")

    # 필요 컬럼 추출
    proj_small = project[["date", "project_id", "company_id"]].copy()
    log_small = logistics[["date", "logistics_id", "project_id", "logistic_create_at", "logistics_status", "logistics_completed_at"]].copy()
    prev_log_small = prev_logistics[["date", "logistics_id", "project_id", "logistics_status"]].copy()

    # 데이터 병합
    log_df = (
      log_small.merge(proj_small, on="project_id", how="left")
    )
    prev_log_df = (
        prev_log_small.merge(proj_small, on="project_id", how="left")
    )

    # 컬럼 타입 정리
    prev_log_df["logistics_status"] = prev_log_df["logistics_status"].astype(str)
    log_df["logistics_status"] = log_df["logistics_status"].astype(str)

    log_df["logistic_create_at"] = pd.to_datetime(log_df["logistic_create_at"], errors="coerce")
    log_df["logistics_completed_at"] = pd.to_datetime(log_df["logistics_completed_at"], errors="coerce")

    as_of_date = pd.to_datetime(proj_small["date"], errors="coerce").max()
    month = str(as_of_date.to_period("M"))
    
    # 시작월, 종료월 컬럼 추가
    log_df["_start_month"] = log_df["logistic_create_at"].dt.to_period("M").astype(str)
    log_df["_end_month"] = log_df["logistics_completed_at"].dt.to_period("M").astype(str)

    # 이월, 당월 시작, 당월 완료 계산
    # map은 company_id 별로 set 형태로 프로젝트 아이디 집합 추출
    carry = prev_log_df.loc[prev_log_df["logistics_status"] != "완료", ["company_id", "logistics_id"]].dropna()  # 이월에 해당하는 상태가 업무할당, 승인대기, 진행 중이라 .. 그냥 완료 아닌 것으로
    carry_map = carry.groupby("company_id")["logistics_id"].apply(lambda s: set(s.unique())).to_dict()

    started = log_df.loc[log_df["_start_month"] == month, ["company_id", "logistics_id"]].dropna()
    started_map = started.groupby("company_id")["logistics_id"].apply(lambda s: set(s.unique())).to_dict()

    completed = log_df.loc[
        (log_df["logistics_status"] == "완료") & (log_df["_end_month"] == month), ["company_id", "logistics_id"]
    ].dropna()
    completed_map = completed.groupby("company_id")["logistics_id"].apply(lambda s: set(s.unique())).to_dict()

    # company id 정리
    all_companies = sorted(set(carry_map.keys()).union(set(started_map.keys())))

    # 출하 완료율 계산
    rows = []
    for cid in all_companies:
        carry_ids = carry_map.get(cid, set())
        started_ids = started_map.get(cid, set())
        eligible_ids = carry_ids.union(started_ids)

        completed_ids = completed_map.get(cid, set())
        completed_eligible_ids = eligible_ids.intersection(completed_ids)

        total_requested = len(eligible_ids)
        completed_cnt = len(completed_eligible_ids)

        completion_rate = np.nan if total_requested == 0 else (completed_cnt / total_requested) * 100.0

        rows.append({
            "month": month,
            "company_id": cid,
            "total_requested_shipping": total_requested,
            "completed_shipping": completed_cnt,
            "shipping_completion_rate": round(float(completion_rate), 3),
        })

    kpi = pd.DataFrame(rows)

    return kpi[["month", "company_id", "shipping_completion_rate"]]