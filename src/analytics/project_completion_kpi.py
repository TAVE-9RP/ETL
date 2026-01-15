import pandas as pd
import numpy as np

def calculate_project_completion_rate(df_project: pd.DataFrame, df_prev_project: pd.DataFrame, target_month: str):
    """
    프로젝트 완료율 계산 (이월 프로젝트 + 당월 시작 프로젝트 대비 당월 완료율)
    """
    # 1. 타입 변환 및 정리
    df = df_project.copy()
    prev_df = df_prev_project.copy()

    df["project_create_date"] = pd.to_datetime(df["project_create_date"], errors="coerce")
    df["project_end_date"] = pd.to_datetime(df["project_end_date"], errors="coerce")
    df["project_status"] = df["project_status"].str.upper()
    prev_df["project_status"] = prev_df["project_status"].str.upper()

    # 2. 분석 대상 월 컬럼 생성
    df["_start_month"] = df["project_create_date"].dt.to_period("M").astype(str)
    df["_end_month"] = df["project_end_date"].dt.to_period("M").astype(str)

    # 3. 상태별 프로젝트 ID 집합 (Set) 생성
    # (1) 이월 건: 전월 스냅샷에서 완료되지 않은 프로젝트 (IN_PROGRESS, NOT_STARTED)
    carry_over = prev_df[prev_df["project_status"] != "COMPLETED"][["company_id", "project_id"]].dropna()
    carry_map = carry_over.groupby("company_id")["project_id"].apply(lambda s: set(s.unique())).to_dict()

    # (2) 당월 신규 건: 시작월이 분석 대상 월과 일치하는 프로젝트
    started = df[df["_start_month"] == target_month][["company_id", "project_id"]].dropna()
    started_map = started.groupby("company_id")["project_id"].apply(lambda s: set(s.unique())).to_dict()

    # (3) 당월 완료 건: 상태가 COMPLETED이고 종료월이 분석 대상 월인 프로젝트
    completed = df[(df["project_status"] == "COMPLETED") & (df["_end_month"] == target_month)][["company_id", "project_id"]].dropna()
    completed_map = completed.groupby("company_id")["project_id"].apply(lambda s: set(s.unique())).to_dict()

    # 4. 회사별 지표 계산
    all_companies = sorted(set(carry_map.keys()).union(set(started_map.keys())))
    results = []

    for cid in all_companies:
        if pd.isna(cid): continue

        eligible_ids = carry_map.get(cid, set()) | started_map.get(cid, set())
        completed_ids = eligible_ids & completed_map.get(cid, set())

        total_count = len(eligible_ids)
        done_count = len(completed_ids)
        rate = (done_count / total_count * 100.0) if total_count > 0 else 0.0

        results.append({
            "company_id": cid,
            "total_requested_projects": total_count,
            "completed_projects": done_count,
            "project_completion_rate": round(float(rate), 3)
        })

    return results