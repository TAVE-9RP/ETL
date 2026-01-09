import pandas as pd
import numpy as np
from config import project_csv, inventory_csv, logistics_csv, inventory_item_csv, logistics_item_csv, item_csv, fix_cols, read_csv


# 월말 안전재고 확보율 계산 함수
def safety_stock_rate_monthly() -> pd.DataFrame:

    project = read_csv(project_csv, fix_cols["project"], "project")
    logistics = read_csv(logistics_csv, fix_cols["logistics"], "logistics")
    inventory = read_csv(inventory_csv, fix_cols["inventory"], "inventory")
    logistics_item = read_csv(logistics_item_csv, fix_cols["logistics_item"], "logistics_item")
    inventory_item = read_csv(inventory_item_csv, fix_cols["inventory_item"], "inventory_item")
    item = read_csv(item_csv, fix_cols["item"], "item")

    # 필요 컬럼 추출
    item_small = item[["date", "item_id", "item_quantity", "safety_stock"]].copy()

    invi_small = inventory_item[["inventory_item_id", "item_id", "inventory_id"]].copy()
    inv_small = inventory[["inventory_id", "project_id"]].copy()

    li_small = logistics_item[["logistics_item_id", "item_id", "logistics_id", "logistics_processed_quantity"]].copy()
    log_small = logistics[["logistics_id", "project_id"]].copy()

    proj_small = project[["project_id", "company_id"]].copy()

    # 입고 데이터 병합 (LEFT JOIN)
    inv_map = (
        invi_small
        .merge(inv_small, on="inventory_id", how="left")
        .merge(proj_small, on="project_id", how="left")
        .dropna(subset=["company_id", "project_id"])
        .drop_duplicates(subset=["item_id"], keep="last")[["item_id", "company_id", "project_id"]]  # item_id 기준 중복 제거
    )   
    
    # 출하 데이터 병합 (LEFT JOIN)
    log_map = (
        li_small
        .merge(log_small, on="logistics_id", how="left")
        .merge(proj_small, on="project_id", how="left")
        .dropna(subset=["company_id", "project_id"])
        .drop_duplicates(subset=["item_id"], keep="last")[["item_id", "company_id", "project_id"]]
    )

    # 입고를 기준으로 출하 데이터 병합
    item_map = (
        inv_map
        .merge(log_map, on="item_id", how="outer", suffixes=("_inv", "_log"))
    )

    # 입고 company id/project id가 있으면 그걸 쓰고, 없으면 출하 company id/project id 사용
    item_map["company_id"] = item_map["company_id_inv"].combine_first(item_map["company_id_log"])
    item_map["project_id"] = item_map["project_id_inv"].combine_first(item_map["project_id_log"])
    item_map = item_map[["item_id", "company_id", "project_id"]]

    # item 기준으로 회사, 프로젝트 매핑 정보 결합
    df = item_small.merge(item_map, on="item_id", how="left")

    # 컬럼 타입 정리
    df["item_quantity"] = pd.to_numeric(df["item_quantity"], errors="coerce")
    df["safety_stock"] = pd.to_numeric(df["safety_stock"], errors="coerce")

    # 날짜 타입 변환 -> 기존 max()에서 datetime으로 바로 변환하는 걸로 로직 변경
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    month = str(df["date"].max().to_period("M"))

    # 데이터 전처리
    # 안전재고는 프로젝트에 들어가는 아이템만
    kpi_base = df.dropna(subset=["company_id", "project_id"]).copy()  # 회사, 프로젝트 없는 항목 제거
    kpi_base["item_quantity"] = kpi_base["item_quantity"].fillna(0)   # 재고량 결측치는 0으로 처리 

    # item_id가 중복되는 경우(한 아이템을 사용하는 물류 업무 여러개) 대비 -> item_id로 그레인 맞추기 (앞에서 맞추긴 했는데 보험 느낌)
    item_level = (
        kpi_base.sort_values(["company_id", "item_id", "date"])
                .drop_duplicates(subset=["company_id", "item_id"], keep="last")
                .copy()
    )

    # 안전재고 확보 여부 파악 -> true: 1 / false: 0
    item_level["secured_flag"] = (item_level["item_quantity"] >= item_level["safety_stock"]).astype(int)

    # 회사-프로젝트별 집계
    kpi = (
        item_level.groupby(["company_id"], as_index=False)
                .agg(
                    total_items=("item_id", "nunique"),
                    secured_items=("secured_flag", "sum"),
                    as_of_date=("date", "max"),   # 월말 스냅샷이면 사실상 그 날짜
                )
    )   

    # 안전재고 확보율 컬럼 추가
    kpi["safety_stock_rate_monthly"] = np.where(
        kpi["total_items"] == 0,
        np.nan,
        kpi["secured_items"] / kpi["total_items"] * 100.0
    ).round(3)

    kpi["month"] = month

    return kpi[["month","company_id", "safety_stock_rate_monthly"]]
