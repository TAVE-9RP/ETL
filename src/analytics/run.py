import pandas as pd
import boto3
import json
# timezone, timedelta 추가
from datetime import datetime, timezone, timedelta
from safety_stock_kpi import calculate_safety_stock_rate
from shipment_lead_time import calculate_shipment_lead_time
from shipping_completion_rate import calculate_shipping_completion_rate
from project_completion_kpi import calculate_project_completion_rate
from long_term_task_rate_kpi import calculate_long_term_task_rate, calculate_leadtimes, build_hist_leadtimes_like_v1
from inventory_turnover import calculate_inventory_turnover
from predict_shipment_lead_time import forecast_lead_time_xgb
from predict_inventory_turnover import forecast_inventory_turnover_hybrid

from config import S3_BUCKET, RAW_PREFIX, KPI_PREFIX, AWS_REGION

s3_client = boto3.client("s3", region_name=AWS_REGION)

def get_last_day_of_month(base_date, month_offset):
    """기준일로부터 n개월 전 마지막 날짜 반환"""
    first_day_of_base = base_date.replace(day=1)
    target_date = first_day_of_base - timedelta(days=1) # 1개월 전 말일
    for _ in range(month_offset - 1):
        target_date = target_date.replace(day=1) - timedelta(days=1)
    return target_date
def get_yesterday(date):
    """현재 날짜 기준 어제의 날짜 반환 (Daily Snapshot 용)"""
    return date - timedelta(days=1)

def get_csv_by_date(table_name, target_date_str):
    """지정된 날짜의 CSV를 S3에서 로드"""
    # 예: exports/daily/item--2025-12-31.csv
    file_key = f"{RAW_PREFIX}{table_name}--{target_date_str}.csv"
    try:
        print(f"[Loading] {file_key}")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
        return pd.read_csv(response['Body'])
    except s3_client.exceptions.NoSuchKey:
        print(f"[Error] File not found: {file_key}")
        return pd.DataFrame()

def get_static_csv(file_name):
    """날짜 접미사 없이 고정된 이름의 CSV 로드"""
    file_key = f"{RAW_PREFIX}{file_name}.csv" # 접미사 제거
    try:
        print(f"[Loading Static] {file_key}")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
        return pd.read_csv(response['Body'])
    except Exception as e:
        print(f"[Error] Static file not found: {file_key}, {e}")
        return pd.DataFrame()
def run():
    kst_timezone = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst_timezone)

    # 1. 날짜 지정
    target_date = get_yesterday(now_kst)
    target_date_str = target_date.strftime("%Y-%m-%d")
    target_month_str = target_date.strftime("%Y-%m")
    print(f"[Target Date] Analyzing data for: {target_date_str}")

    first_day_of_current = target_date.replace(day=1)
    first_day_str = first_day_of_current.strftime("%Y-%m-%d")
    prev_month_end = first_day_of_current - timedelta(days=1)
    prev_month_end_str = prev_month_end.strftime("%Y-%m-%d")

    # 2. 데이터 로드
    df_item = get_csv_by_date("item", target_date_str)
    df_project = get_csv_by_date("project", target_date_str)
    df_prev_project = get_csv_by_date("project", prev_month_end_str) # 전월 말 프로젝트 스냅샷
    df_logistics = get_csv_by_date("logistics", target_date_str)
    df_logistics_item = get_csv_by_date("logistics_item", target_date_str)
    df_prev_logistics = get_csv_by_date("logistics", prev_month_end_str) # 전월 말 출하 스냅샷
    df_inventory = get_csv_by_date("inventory", target_date_str)
    df_inventory_item = get_csv_by_date("inventory_item", target_date_str)

    # 재고 회전율의 (월초 - 회전율 계산용)
    df_item_f = get_csv_by_date("item", first_day_str)
    df_project_f = get_csv_by_date("project", first_day_str)
    df_logistics_f = get_csv_by_date("logistics", first_day_str)
    df_logistics_item_f = get_csv_by_date("logistics_item", first_day_str)
    df_inventory_f = get_csv_by_date("inventory", first_day_str)
    df_inventory_item_f = get_csv_by_date("inventory_item", first_day_str)

    # 재고 회전율 월초 데이터 딕셔너리로 묶기
    df_first_dict = {
        'project': df_project_f,
        'inventory': df_inventory_f,
        'inventory_item': df_inventory_item_f,
        'logistics': df_logistics_f,
        'logistics_item': df_logistics_item_f,
        'item': df_item_f
    }

    # 재고 회전율 월말 데이터 딕셔너리로 묶기
    df_last_dict = {
        'project': df_project,
        'inventory': df_inventory,
        'inventory_item': df_inventory_item,
        'logistics': df_logistics,
        'logistics_item': df_logistics_item,
        'item': df_item
    }

    # 예측용 Mock 데이터 로드
    df_leadtime_mock = get_static_csv("predict_leadtime_mock")
    df_turnover_mock = get_static_csv("predict_turnover_mock")

    # 과거 3개월 데이터 로드 (업무 장기 처리율 SLA 계산용)
    hist_logs, hist_invs = [], []
    for i in range(1, 4):
        hist_date_str = get_last_day_of_month(target_date, i).strftime("%Y-%m-%d")

        h_proj = get_csv_by_date("project", hist_date_str)
        h_log = get_csv_by_date("logistics", hist_date_str)
        h_inv = get_csv_by_date("inventory", hist_date_str)

        if not h_log.empty and not h_inv.empty:
            h_log_processed, h_inv_processed = build_hist_leadtimes_like_v1(h_proj, h_log, h_inv)

        # 가공된(필터링 완료된) 리드타임 데이터프레임을 리스트에 추가
        hist_logs.append(h_log_processed)
        hist_invs.append(h_inv_processed)
    # [디버깅 추가] 데이터 로드 확인
    print(f"로드 결과: item({len(df_item)}건), project({len(df_project)}건), logistics({len(df_logistics)}건), logistics_item({len(df_logistics_item)}건)")

    if df_item.empty:
        raise RuntimeError(f"데이터가 없어 분석을 진행할 수 없습니다: {target_date_str}")

    # 3. KPI 분석
    safety_results = calculate_safety_stock_rate(df_project, df_inventory, df_inventory_item, df_logistics, df_logistics_item, df_item)
    lead_time_results = calculate_shipment_lead_time(df_project, df_logistics, target_month_str)
    log_comp_results = calculate_shipping_completion_rate(df_project, df_logistics, df_prev_logistics, target_month_str)
    proj_comp_results = calculate_project_completion_rate(df_project, df_prev_project, target_month_str)
    long_term_results = calculate_long_term_task_rate(df_project, df_logistics, df_inventory, hist_logs, hist_invs, target_month_str)
    turn_over_results = calculate_inventory_turnover(df_first_dict, df_last_dict)
    pred_lead_time_results = forecast_lead_time_xgb(df_leadtime_mock)
    pred_turnover_results = forecast_inventory_turnover_hybrid(df_turnover_mock)
    # 4. 데이터 병합
    combined_kpis = {}

    # 초기화 및 안전재고 확보율 병합
    for item in safety_results:
        cid = item["company_id"]
        combined_kpis.setdefault(cid, {})["safety_stock_rate"] = item["safety_stock_rate_monthly"]

    # 출하 리드타임 평균 병합
    for item in lead_time_results:
        cid = item["company_id"]
        combined_kpis.setdefault(cid, {})["shipment_lead_time"] = item["shipment_lead_time_avg_hours"]

    # 출하 완료율 병합 (신규 추가)
    for item in log_comp_results:
        cid = item["company_id"]
        combined_kpis.setdefault(cid, {})["shipping_completion_rate"] = item["shipping_completion_rate"]

    # 프로젝트 완료율 병합
    for item in proj_comp_results:
        cid = item["company_id"]
        combined_kpis.setdefault(cid, {})["project_completion_rate"] = item["project_completion_rate"]

    # 업무 장기 처리율 병합
    for item in long_term_results:
        cid = item["company_id"]
        target = combined_kpis.setdefault(cid, {})
        target["long_term_task_rate"] = item["long_term_task_rate"]
        target["total_task_count"] = item["total_task_count"]
        target["logistics_task_count"] = item["logistics_task_count"]
        target["inventory_task_count"] = item["inventory_task_count"]
        target["total_delayed_count"] = item["total_delayed_count"]
        target["logistics_delayed_count"] = item["logistics_delayed_count"]
        target["inventory_delayed_count"] = item["inventory_delayed_count"]

    # 재고 회전율 병합
    for item in turn_over_results:
        cid = item["company_id"]
        combined_kpis.setdefault(cid, {})["inventory_turnover"] = item["inventory_turnover"]

    print(f"분석 완료: 총 {len(combined_kpis)}개 회사의 통합 KPI가 산출되었습니다.")

    # 리드타임 예측치 병합
    for item in pred_lead_time_results:
        cid = item["company_id"]
        combined_kpis.setdefault(cid, {})["pred_shipment_lead_time"] = item["pred_shipment_lead_time"]

    # 재고회전율 예측치 병합
    for item in pred_turnover_results:
        cid = int(item["company_id"])
        combined_kpis.setdefault(cid, {})["pred_inventory_turnover"] = item["pred_inventory_turnover"]

    # 5. 결과 S3 저장 (JSON 적재)
    for company_id, metrics in combined_kpis.items():
        if pd.isna(company_id): continue

        payload = {
            "companyId": int(company_id),
            "snapshotDate": target_date_str,
            "metrics": {
                "safetyStockRate": float(metrics.get("safety_stock_rate", 0.0)),
                "shipmentLeadTimeAvg": float(metrics.get("shipment_lead_time", 0.0)),
                "shippingCompletionRate": float(metrics.get("shipping_completion_rate", 0.0)),
                "projectCompletionRate": float(metrics.get("project_completion_rate", 0.0)),
                "longTermTaskRate": float(metrics.get("long_term_task_rate", 0.0)),
                "totalTaskCount": int(metrics.get("total_task_count", 0)),
                "logisticsTaskCount": int(metrics.get("logistics_task_count", 0)),
                "inventoryTaskCount": int(metrics.get("inventory_task_count", 0)),
                "totalDelayedCount": int(metrics.get("total_delayed_count", 0)),
                "logisticsDelayedCount": int(metrics.get("logistics_delayed_count", 0)),
                "inventoryDelayedCount": int(metrics.get("inventory_delayed_count", 0)),
                "turnOverRate": float(metrics.get("inventory_turnover", 0.0)),
                "predShipmentLeadTime": float(metrics.get("pred_shipment_lead_time", 0.0)),
                "predTurnOverRate": float(metrics.get("pred_inventory_turnover", 0.0))
            },
            "calculatedAt": now_kst.isoformat()
        }

        # 저장 경로 (Daily Report 통합)
        key = f"{KPI_PREFIX}/daily-report/company-{int(company_id)}/report_{target_date_str}.json"

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False),
            ContentType="application/json"
        )
        print(f"[Success] Uploaded Integrated KPI -> {key}")

def lambda_handler(event, context):
    try:
        run()
        return {'statusCode': 200, 'body': json.dumps('ETL Job successfully completed')}
    except Exception as e:
        print(f"ETL failed: {str(e)}")
        raise e

if __name__ == "__main__":
    run()