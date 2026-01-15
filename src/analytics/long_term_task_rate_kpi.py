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

import pandas as pd
import numpy as np

def snapshot_month_from_date(df_tasks: pd.DataFrame) -> str:
    """파일의 date 컬럼 최댓값에서 'YYYY-MM' 형식의 월 추출"""
    if df_tasks.empty: return ""
    max_date = pd.to_datetime(df_tasks["date"], errors="coerce").max()
    return str(max_date.to_period("M"))

def build_hist_leadtimes_like_v1(df_project_hist, df_log_hist, df_inv_hist):
    """각 과거 스냅샷 파일별로 '자기 월'의 데이터만 정확히 추출"""
    m = snapshot_month_from_date(df_log_hist)
    hist_log = calculate_leadtimes(df_project_hist, df_log_hist, "logistics", m)
    hist_inv = calculate_leadtimes(df_project_hist, df_inv_hist, "inventory", m)
    return hist_log, hist_inv

def calculate_sla_like_v1(all_hist_log: pd.DataFrame, all_hist_inv: pd.DataFrame) -> pd.DataFrame:
    """분석팀과 동일하게 groupby quantile을 사용하여 회사별 SLA(P80) 산출"""
    # 리드타임 0 초과 데이터만 전처리
    if not all_hist_log.empty:
        all_hist_log = all_hist_log[all_hist_log["lead_time"].notna() & (all_hist_log["lead_time"] > 0)]
    if not all_hist_inv.empty:
        all_hist_inv = all_hist_inv[all_hist_inv["lead_time"].notna() & (all_hist_inv["lead_time"] > 0)]

    log_grp = all_hist_log.groupby("company_id")["lead_time"] if not all_hist_log.empty else None
    inv_grp = all_hist_inv.groupby("company_id")["lead_time"] if not all_hist_inv.empty else None

    log_sla = pd.DataFrame({
        "company_id": log_grp.size().index,
        "log_p80": log_grp.quantile(0.8).values,
    }) if log_grp is not None else pd.DataFrame(columns=["company_id", "log_p80"])

    inv_sla = pd.DataFrame({
        "company_id": inv_grp.size().index,
        "inv_p80": inv_grp.quantile(0.8).values,
    }) if inv_grp is not None else pd.DataFrame(columns=["company_id", "inv_p80"])

    return log_sla.merge(inv_sla, on="company_id", how="outer")

def add_is_over_column(target_df: pd.DataFrame, df_sla: pd.DataFrame, sla_col_name: str):
    """Merge 방식을 사용하여 인덱스 꼬임 방지 및 NaN 처리 유지"""
    if target_df.empty or df_sla.empty:
        res = target_df.copy()
        res["is_over"] = False
        return res

    merged = target_df.merge(df_sla[["company_id", sla_col_name]], on="company_id", how="left")

    # SLA 기준이 없으면 NaN 유지 (sum 시 자동 제외)
    merged["is_over"] = np.where(
        merged[sla_col_name].notna(),
        merged["lead_time"] > merged[sla_col_name],
        np.nan
    )
    return merged

def calculate_long_term_task_rate(df_project: pd.DataFrame, df_log: pd.DataFrame, df_inv: pd.DataFrame,
                                  hist_logs: list, hist_invs: list, target_month: str):
    # 1. 현재 월 데이터 추출
    cur_log = calculate_leadtimes(df_project, df_log, "logistics", target_month)
    cur_inv = calculate_leadtimes(df_project, df_inv, "inventory", target_month)

    # 2. 통합 SLA 데이터프레임 생성
    all_hist_log = pd.concat(hist_logs, ignore_index=True) if hist_logs else pd.DataFrame()
    all_hist_inv = pd.concat(hist_invs, ignore_index=True) if hist_invs else pd.DataFrame()
    df_sla = calculate_sla_like_v1(all_hist_log, all_hist_inv)

    # 3. 초과 여부 판단 (Merge 기반)
    cur_log = add_is_over_column(cur_log, df_sla, "log_p80")
    cur_inv = add_is_over_column(cur_inv, df_sla, "inv_p80")

    # 4. 회사별 최종 집계
    results = []
    for cid in df_project["company_id"].unique():
        if pd.isna(cid): continue

        c_log = cur_log[cur_log["company_id"] == cid]
        c_inv = cur_inv[cur_inv["company_id"] == cid]

        total_tasks = len(c_log) + len(c_inv)
        # sum()은 NaN을 무시하고 계산함
        total_over = c_log["is_over"].sum() + c_inv["is_over"].sum()

        rate = (total_over / total_tasks * 100) if total_tasks > 0 else 0.0

        results.append({
            "company_id": int(cid),
            "long_term_task_rate": round(float(rate), 3),
            "total_task_count": int(total_tasks),
            "total_delayed_count": int(total_over),
            "logistics_task_count": len(c_log),
            "inventory_task_count": len(c_inv),
            "logistics_delayed_count": int(c_log["is_over"].sum()),
            "inventory_delayed_count": int(c_inv["is_over"].sum())
        })
    return results