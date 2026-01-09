import pandas as pd
import numpy as np

def calculate_shipping_completion_rate(df_project: pd.DataFrame, df_logistics: pd.DataFrame, df_prev_logistics: pd.DataFrame, target_month: str):
    """
    출하 완료율 계산
    대상: 전월 이월 건 + 당일 신규 생성 건 중 당월 내 완료 건수
    """
    # 1. 데이터 전처리 (현재월 및 프로젝트 정보 조인)
    df = df_logistics.merge(df_project[['project_id', 'company_id']], on='project_id', how='left')

    # 2. 전월 말 데이터 처리 (이월 건 파악용)
    # 전월 말 기준으로 'COMPLETED'가 아닌 것들은 모두 이월 대상으로 간주
    prev_df = df_prev_logistics.merge(df_project[['project_id', 'company_id']], on='project_id', how='left')
    prev_df['logistics_status'] = prev_df['logistics_status'].str.upper()

    # 3. 날짜 및 상태값 정규화
    df['logistic_created_at'] = pd.to_datetime(df['logistic_created_at'], errors='coerce')
    df['logistics_completed_at'] = pd.to_datetime(df['logistics_completed_at'], errors='coerce')
    df['logistics_status'] = df['logistics_status'].str.upper()

    # 분석 대상 월 추출
    df['_start_month'] = df['logistic_created_at'].dt.to_period('M').astype(str)
    df['_end_month'] = df['logistics_completed_at'].dt.to_period('M').astype(str)

    # 4. 집계 대상 분류
    # (1) 이월 건 (carry-over): 전월 말일 데이터 중 미완료 상태인 건들
    carry_over = prev_df[prev_df['logistics_status'] != 'COMPLETED'][['company_id', 'logistics_id']]

    # (2) 당월 신규 건 (New orders): 이번 달에 생성된 건들
    new_orders = df[df['_start_month'] == target_month][['company_id', 'logistics_id']]

    # (3) 당월 완료 건 (Completed in Month): 이번 달에 완료 상태로 변경된 건들
    completed_in_month = df[
        (df['logistics_status'] == 'COMPLETED') &
        (df['_end_month'] == target_month)
    ][['company_id', 'logistics_id']]

    # 5. 회사별 KPI 계산
    results = []
    all_companies = set(carry_over['company_id'].unique()) | set(new_orders['company_id'].unique())

    for cid in all_companies:
        if pd.isna(cid): continue

        # 해당 회사의 이월 ID와 신규 ID 합집합 (전체 요청 대상)
        c_carry = set(carry_over[carry_over['company_id'] == cid]['logistics_id'])
        c_new = set(new_orders[new_orders['company_id'] == cid]['logistics_id'])
        eligible_ids = c_carry | c_new

        # 이번 달 완료된 ID 중 요청 대상에 포함된 것 필터링
        c_completed = set(completed_in_month[completed_in_month['company_id'] == cid]['logistics_id'])
        completed_ids = eligible_ids & c_completed

        total_count = len(eligible_ids)
        done_count = len(completed_ids)

        rate = (done_count / total_count * 100.0) if total_count > 0 else 0.0

        results.append({
            "company_id": cid,
            "total_requested_shipping": total_count,
            "completed_shipping": done_count,
            "shipping_completion_rate": round(rate, 3)
        })
    return results