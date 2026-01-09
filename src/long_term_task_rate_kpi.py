import pandas as pd
import numpy as np

def calculate_leadtimes(df_project: pd.DataFrame, df_tasks: pd.DataFrame, task_type: str, target_month: str):
    """
    개별 업무의 (Logistics 또는 Inventory) 리드 타임을 계산하는 공통 함수
    """

    # 0. 데이터가 비어있는 경우 빈 데이터프레임 반환
    if df_tasks.empty or df_project.empty:
        return pd.DataFrame(columns=["company_id", "task_id", "lead_time"])

    # 1. 컬럼 매핑 정보 설정 (입고 / 출하 구분)
    col_map = {
        "logistics": {"id": "logistics_id", "start": "logistic_created_at", "end": "logistics_completed_at", "status": "logistics_status"},
        "inventory": {"id": "inventory_id", "start": "inventory_created_at", "end": "inventory_completed_at", "status": "inventory_status"}
    }[task_type]

    # 2. 데이터 병합 및 타입 변환
    df = df_tasks.merge(df_project[["project_id", "company_id"]], on="project_id", how="left")

    df[col_map["start"]] = pd.to_datetime(df[col_map["start"]], errors="coerce")
    df[col_map["end"]] = pd.to_datetime(df[col_map["end"]], errors="coerce")
    df[col_map["status"]] = df[col_map["status"]].str.upper()

    # 3. 분석 대상 월 필터링 (완료일 기준)
    df["_end_month"] = df[col_map["end"]].dt.to_period("M").astype(str)

    mask = (df[col_map["status"]] == "COMPLETED") & \
           (df[col_map["start"]].notna()) & \
           (df[col_map["end"]].notna()) & \
           (df["_end_month"] == target_month)

    df_valid = df[mask].copy()

    if df_valid.empty:
        return pd.DataFrame(columns=["company_id", "task_id", "lead_time"])

    # 4. 리드타임 계산 (시간 단위)
    df_valid["lead_time"] = (df_valid[col_map["end"]] - df_valid[col_map["start"]]).dt.total_seconds() / 3600.0

    return df_valid.rename(columns={col_map["id"]: "task_id"})[["company_id", "task_id", "lead_time"]]

def calculate_long_term_task_rate(df_project: pd.DataFrame, df_log: pd.DataFrame, df_inv: pd.DataFrame,
                                  hist_logs: list, hist_invs: list, target_month: str):
    """
    과거 3개월 SLA(80th Percentile) 기준 업무 장기 처리율 계산, KeyError 방지 로직 포함
    """
    # 1. 현재 월 리드타임 계산
    cur_log = calculate_leadtimes(df_project, df_log,"logistics", target_month)
    cur_inv = calculate_leadtimes(df_project, df_inv, "inventory", target_month)

    # 2. 과거 3개월 데이터 통합 및 SLA(P80) 산출
    # 과거 데이터들도 각각의 완료 월 기준으로 리드타임이 미리 계산되어 있어야 함 (run.py 에서 처리)
    all_hist_log = pd.concat(hist_logs, ignore_index=True) if hist_logs else pd.DataFrame()
    all_hist_inv = pd.concat(hist_invs, ignore_index=True) if hist_invs else pd.DataFrame()

    sla_data = []
    # 회사별로 SLA 기준치 매핑
    for cid in df_project["company_id"].unique():
        if pd.isna(cid): continue
        log_p80 = all_hist_log[all_hist_log["company_id"] == cid]["lead_time"].quantile(0.8)
        inv_p80 = all_hist_inv[all_hist_inv["company_id"] == cid]["lead_time"].quantile(0.8)
        sla_data.append({"company_id": cid, "log_p80": log_p80, "inv_p80": inv_p80})

    df_sla = pd.DataFrame(sla_data)

    # 'is_over' 계산용 컬럼 생성
    # 병합 전에 컬럼을 미리 생성하거나, 병합 후 fillna를 통해 보장
    def add_is_over_column(target_df, sla_col_name):
        if target_df.empty or df_sla.empty:
            target_df["is_over"] = False # 데이터가 없으면 모두 정상 처리
            return target_df

        merged = target_df.merge(df_sla[["company_id", sla_col_name]], on="company_id", how="left")
        # 기준치(P80)가 없으면 (NaN) 초과하지 않은 것으로 간주
        target_df["is_over"] = (merged["lead_time"] > merged[sla_col_name]).fillna(False)
        return target_df

    # 3. 현재 월 데이터와 SLA 결합 및 초과 여부 판단
    cur_log = add_is_over_column(cur_log, "log_p80")
    cur_inv = add_is_over_column(cur_inv, "inv_p80")

    # 4. 회사별 최종 집계
    results = []
    for cid in df_project["company_id"].unique():
        if pd.isna(cid): continue

        c_log = cur_log[cur_log["company_id"] == cid]
        c_inv = cur_inv[cur_inv["company_id"] == cid]

        total_tasks = len(c_log) + len(c_inv)
        over_tasks = c_log["is_over"].sum() + c_inv["is_over"].sum()

        rate = (over_tasks / total_tasks * 100) if total_tasks > 0 else 0.0

        results.append({
            "company_id": cid,
            "long_term_task_rate": round(float(rate), 3)
        })

    return results