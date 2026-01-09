import pandas as pd
import numpy as np
from config import project_csv, inventory_csv, logistics_csv, inventory_item_csv, logistics_item_csv, item_csv, fix_cols, read_csv

first_project_csv = "project--2026-01-01.csv"
first_logistics_csv = "logistics--2026-01-01.csv"
first_inventory_csv = "inventory--2026-01-01.csv"
first_logistics_item_csv = "logistics_item--2026-01-01.csv"
first_inventory_item_csv = "inventory_item--2026-01-01.csv"
first_item_csv = "item--2026-01-01.csv"

# 월말 재고 회전율 계산 함수
def turnover_monthly() -> pd.DataFrame:

    # 로직 구분하기 -> first: 월초 / last: 월말
    first_project = read_csv(first_project_csv, fix_cols["project"], "first_project")
    first_logistics = read_csv(first_logistics_csv, fix_cols["logistics"], "first_logistics")
    first_logistics_item = read_csv(first_logistics_item_csv, fix_cols["logistics_item"], "first_logistics_item")
    first_inventory = read_csv(first_inventory_csv, fix_cols["inventory"], "first_inventory")
    first_inventory_item = read_csv(first_inventory_item_csv, fix_cols["inventory_item"], "first_inventory_item")
    first_item = read_csv(first_item_csv, fix_cols["item"], "first_item")

    last_project = read_csv(project_csv, fix_cols["project"], "project")
    last_logistics = read_csv(logistics_csv, fix_cols["logistics"], "logistics")
    last_logistics_item = read_csv(logistics_item_csv, fix_cols["logistics_item"], "logistics_item")
    last_inventory = read_csv(inventory_csv, fix_cols["inventory"], "inventory")
    last_inventory_item = read_csv(inventory_item_csv, fix_cols["inventory_item"], "inventory_item")
    last_item = read_csv(item_csv, fix_cols["item"], "item")

    # 필요 컬럼 추출
    first_item_small = first_item[["date","item_id", "item_quantity", "safety_stock"]].copy()
    first_invi_small = first_inventory_item[["inventory_item_id", "item_id", "inventory_id"]].copy()
    first_inv_small = first_inventory[["inventory_id", "project_id"]].copy()
    first_li_small = first_logistics_item[["logistics_item_id", "item_id", "logistics_id", "logistics_processed_quantity"]].copy()
    first_log_small = first_logistics[["logistics_id", "project_id"]].copy()
    first_proj_small = first_project[["project_id", "company_id"]].copy()

    last_item_small = last_item[["date", "item_id", "item_quantity", "safety_stock"]].copy()
    last_invi_small = last_inventory_item[["inventory_item_id", "item_id", "inventory_id"]].copy()
    last_inv_small = last_inventory[["inventory_id", "project_id"]].copy()
    last_li_small = last_logistics_item[["logistics_item_id", "item_id", "logistics_id", "logistics_processed_quantity"]].copy()
    last_log_small = last_logistics[["logistics_id", "project_id"]].copy()
    last_proj_small = last_project[["project_id", "company_id"]].copy()

    # 입고 데이터 병합 (LEFT JOIN)
    first_inv_map = (
        first_invi_small
        .merge(first_inv_small, on="inventory_id", how="left")
        .merge(first_proj_small, on="project_id", how="left")
        .dropna(subset=["company_id", "project_id"])
    )

    last_inv_map = (
        last_invi_small
        .merge(last_inv_small, on="inventory_id", how="left")
        .merge(last_proj_small, on="project_id", how="left")
        .dropna(subset=["company_id", "project_id"])
    )
    
    # 출하 데이터 병합 (LEFT JOIN)
    first_log_map = (
        first_li_small
        .merge(first_log_small, on="logistics_id", how="left")
        .merge(first_proj_small, on="project_id", how="left")
        .dropna(subset=["company_id", "project_id"])
    )


    last_log_map = (
        last_li_small
        .merge(last_log_small, on="logistics_id", how="left")
        .merge(last_proj_small, on="project_id", how="left")
        .dropna(subset=["company_id", "project_id"])
    )

    # 입고를 기준으로 출하 데이터 병합
    first_log_map1 = first_log_map.drop_duplicates(subset=["item_id"], keep="last")[["item_id", "company_id", "project_id"]]
    last_log_map1 = last_log_map.drop_duplicates(subset=["item_id"], keep="last")[["item_id", "company_id", "project_id"]]

    first_inv_map1 = first_inv_map.drop_duplicates(subset=["item_id"], keep="last")[["item_id", "company_id", "project_id"]]
    last_inv_map1 = last_inv_map.drop_duplicates(subset=["item_id"], keep="last")[["item_id", "company_id", "project_id"]]

    first_item_map = (
        first_inv_map1
        .merge(first_log_map1, on="item_id", how="outer", suffixes=("_inv", "_log"))
    )

    last_item_map = (
        last_inv_map1
        .merge(last_log_map1, on="item_id", how="outer", suffixes=("_inv", "_log"))
    )

    # 입고 company id/project id가 있으면 그걸 쓰고, 없으면 출하 company id/project id 사용
    first_item_map["company_id"] = first_item_map["company_id_inv"].combine_first(first_item_map["company_id_log"])
    first_item_map["project_id"] = first_item_map["project_id_inv"].combine_first(first_item_map["project_id_log"])
    first_item_map = first_item_map[["item_id", "company_id", "project_id"]]

    last_item_map["company_id"] = last_item_map["company_id_inv"].combine_first(last_item_map["company_id_log"])
    last_item_map["project_id"] = last_item_map["project_id_inv"].combine_first(last_item_map["project_id_log"])
    last_item_map = last_item_map[["item_id", "company_id", "project_id"]]

    # item 기준으로 회사, 프로젝트 매핑 정보 결합
    first_df = first_item_small.merge(first_item_map, on="item_id", how="left")
    last_df = last_item_small.merge(last_item_map, on="item_id", how="left")

    # 컬럼 타입 정리
    first_df["item_quantity"] = pd.to_numeric(first_df["item_quantity"], errors="coerce").fillna(0)
    last_df["item_quantity"]  = pd.to_numeric(last_df["item_quantity"], errors="coerce").fillna(0)

    # 날짜 타입 변환 -> 기존 max()에서 datetime으로 바로 변환하는 걸로 로직 변경
    last_df["date"] = pd.to_datetime(last_df["date"], errors="coerce")
    month = str(last_df["date"].max().to_period("M"))

    # 데이터 전처리
    # company_id가 있는 데이터로 추출 -> 근데 없을리가 없어서 사실상 전체 데이터
    first_inv_base = first_df.dropna(subset=["company_id"]).copy()
    last_inv_base  = last_df.dropna(subset=["company_id"]).copy()

    # item_id가 중복되는 경우(한 아이템을 사용하는 물류 업무 여러개) 대비 -> item_id로 그레인 맞추기
    first_item_level = (
    first_inv_base.sort_values(["company_id", "item_id", "date"])
                    .drop_duplicates(subset=["company_id", "item_id"], keep="last")
    )
    last_item_level = (
        last_inv_base.sort_values(["company_id", "item_id", "date"])
                    .drop_duplicates(subset=["company_id", "item_id"], keep="last")
    )   

    # 회사별 재고량 집계 -> 평균 재고량 계산
    begin_inv = first_item_level.groupby("company_id", as_index=False).agg(begin_inventory=("item_quantity", "sum"))
    end_inv = last_item_level.groupby("company_id", as_index=False).agg(end_inventory=("item_quantity", "sum"))

    inv = begin_inv.merge(end_inv, on="company_id", how="outer")
    inv["begin_inventory"] = inv["begin_inventory"].fillna(0)
    inv["end_inventory"]   = inv["end_inventory"].fillna(0)
    inv["avg_inventory"]   = (inv["begin_inventory"] + inv["end_inventory"]) / 2

    # 회사별 출하량 집계
    first_log_map["logistics_processed_quantity"] = pd.to_numeric(first_log_map["logistics_processed_quantity"], errors="coerce").fillna(0)
    first_ship = (
        first_log_map.groupby(["company_id", "logistics_item_id"], as_index=False)   # 출하 업무 기준 출하량 파악
                    .agg(first_ship=("logistics_processed_quantity", "max"))
    )

    last_log_map["logistics_processed_quantity"] = pd.to_numeric(last_log_map["logistics_processed_quantity"], errors="coerce").fillna(0)
    last_ship = (
        last_log_map.groupby(["company_id", "logistics_item_id"], as_index=False)
                    .agg(last_ship=("logistics_processed_quantity", "max"))
    )

    shipment = last_ship.merge(first_ship, on=["company_id", "logistics_item_id"], how="left")
    shipment["first_ship"] = shipment["first_ship"].fillna(0)
    shipment["delta_ship"] = (shipment["last_ship"] - shipment["first_ship"]).clip(lower=0)

    ship = (
        shipment.groupby("company_id", as_index=False)
            .agg(month_ship=("delta_ship", "sum"))
    )

    # 재고 회전율 계산
    kpi = inv.merge(ship, on="company_id", how="outer")
    kpi["month_ship"] = kpi["month_ship"].fillna(0)

    kpi["turnover_monthly"] = np.where(
        kpi["avg_inventory"] == 0,
        np.nan,
        kpi["month_ship"] / kpi["avg_inventory"]
    ).round(3)


    kpi["month"] = month

    return kpi[["month", "company_id", "turnover_monthly"]]