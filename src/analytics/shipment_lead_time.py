import pandas as pd
import numpy as np

def calculate_shipment_lead_time(df_project: pd.DataFrame, df_logistics: pd.DataFrame, target_month: str):
    """
    당월(target_month)에 완료된 물류를 대상으로 생성부터 완료까지의 평균 소요 시간 계산
    """

    # 1. 데이터 병합 (logistics + project)
    df = df_logistics.merge(df_project[["project_id", "company_id"]], on="project_id", how="left")

    # 2. 날짜 타입 변환 및 상태값 대문자 정규화
    df["logistic_created_at"] = pd.to_datetime(df["logistic_created_at"], errors="coerce")
    df["logistics_completed_at"] = pd.to_datetime(df["logistics_completed_at"], errors="coerce")
    df["logistics_status"] = df["logistics_status"].str.upper()

    # 3. 완료 시점의 '월' 추출 (DA 팀 최신 로직 반영)
    df["_end_month"] = df["logistics_completed_at"].dt.to_period("M").astype(str)

    # 4. 분석 대상 필터링
    # - 상태가 COMPLETED
    # - 시작/완료일이 유효함
    # - 완료일이 시작일보다 늦음 (역전 방지)
    # - 완료된 달이 분석 대상 월(target_month)과 일치함
    valid_mask = (
        (df["logistics_status"] == "COMPLETED") &
        (df["logistic_created_at"].notna()) &
        (df["logistics_completed_at"].notna()) &
        (df["logistics_completed_at"] >= df["logistic_created_at"]) &
        (df["_end_month"] == target_month)
    )

    df_valid = df[valid_mask].copy()

    # 5. 리드타임 계산 (단위: 시간)
    df_valid["lead_time_hours"] = (
        (df_valid["logistics_completed_at"] - df_valid["logistic_created_at"])
        .dt.total_seconds() / 3600.0
    )

    # 6. 회사별 집계 (평균값 계산)
    kpi = (
        df_valid.groupby("company_id", as_index=False)
            .agg(
                shipment_lead_time_avg_hours=("lead_time_hours", "mean"),
                completed_count=("logistics_id", "count")
        )
    )

    # 7. 소수점 정리 및 반환
    kpi["shipment_lead_time_avg_hours"] = kpi["shipment_lead_time_avg_hours"].round(2)

    return kpi.to_dict(orient="records")
