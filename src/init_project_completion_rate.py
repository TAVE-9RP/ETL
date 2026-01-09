import pandas as pd
import numpy as np
from config import project_csv, fix_cols, read_csv

# 이전월 프로젝트 스냅샷 파일 경로
prev_month_csv = "project--2025-12-31.csv"

# 월말 프로젝트 완료율 계산 함수
def project_completion_rate_monthly() -> pd.DataFrame:

    project = read_csv(project_csv, fix_cols["project"], "project")
    prev_project = read_csv(prev_month_csv, fix_cols["project"], "prev_project")

    # 필요 컬럼 추출
    proj_small = project[["date", "project_id", "company_id", "project_create_date", "project_end_date"]].copy()
    prev_proj_small = prev_project[["date", "project_id", "company_id", "project_status"]].copy()

    # 컬럼 타입 정리
    prev_proj_small["project_status"] = prev_proj_small["project_status"].astype(str)

    proj_small["company_id"] = proj_small["company_id"].astype(str)
    proj_small["project_id"] = proj_small["project_id"].astype(str)
    prev_proj_small["company_id"] = prev_proj_small["company_id"].astype(str)
    prev_proj_small["project_id"] = prev_proj_small["project_id"].astype(str)

    proj_small["project_create_date"] = pd.to_datetime(proj_small["project_create_date"], errors="coerce")
    proj_small["project_end_date"] = pd.to_datetime(proj_small["project_end_date"], errors="coerce")

    as_of_date = pd.to_datetime(proj_small["date"], errors="coerce").max()
    month = str(as_of_date.to_period("M"))

    # 시작월, 종료월 컬럼 추가
    proj_small["_start_month"] = proj_small["project_create_date"].dt.to_period("M").astype(str)
    proj_small["_end_month"] = proj_small["project_end_date"].dt.to_period("M").astype(str)

    # 이월, 당월 시작, 당월 완료 계산
    # map은 company_id 별로 set 형태로 프로젝트 아이디 집합 추출
    carry = prev_proj_small.loc[prev_proj_small["project_status"] == "진행 중", ["company_id", "project_id"]].dropna()
    carry_map = carry.groupby("company_id")["project_id"].apply(lambda s: set(s.unique())).to_dict()

    started = proj_small.loc[proj_small["_start_month"] == month, ["company_id", "project_id"]].dropna()
    started_map = started.groupby("company_id")["project_id"].apply(lambda s: set(s.unique())).to_dict()

    completed = proj_small.loc[
        (proj_small["project_status"] == "완료") & (proj_small["_end_month"] == month), ["company_id", "project_id"]
    ].dropna()
    completed_map = completed.groupby("company_id")["project_id"].apply(lambda s: set(s.unique())).to_dict()

    # company id 정리
    all_companies = sorted(set(carry_map.keys()).union(set(started_map.keys())))

    # 프로젝트 완료율 계산
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
            "total_requested_projects": total_requested,
            "completed_projects": completed_cnt,
            "project_completion_rate": round(float(completion_rate), 3),
        })

    kpi = pd.DataFrame(rows)

    return kpi[["month", "company_id", "project_completion_rate"]]

