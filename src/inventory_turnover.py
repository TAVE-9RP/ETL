import pandas as pd
import numpy as np

def calculate_inventory_turnover(df_first: dict, df_last: dict):
    """
    재고 회전율 계산
    df_first: 월초 데이터프레임들을 담은 딕셔너리
    df_last: 월말(현재) 데이터프레임들을 담은 딕셔너리
    """

    def get_item_company_map(project, inventory, inventory_item, logistics, logistics_item):
        """품목별 회사 매핑 """
        inv_map = inventory_item.merge(inventory, on="inventory_id", how="left") \
                                .merge(project, on="project_id", how="left") \
                                .dropna(subset=["company_id"]) \
                                .drop_duplicates(subset=["item_id"], keep="last")
        log_map = logistics_item.merge(logistics, on="logistics_id", how="left") \
                                .merge(project, on="project_id", how="left") \
                                .drop_duplicates(subset=["item_id"], keep="last")[["item_id", "company_id"]]

        item_map = inv_map.merge(log_map, on="item_id", how="outer", suffixes=("_inv", "_log"))
        item_map["company_id"] = item_map["company_id_inv"].combine_first(item_map["company_id_log"])
        return item_map[["item_id", "company_id"]]

    # 1. 월초/월말 아이템-회사 매핑 생성
    first_map = get_item_company_map(df_first['project'], df_first['inventory'], df_first['inventory_item'], df_first['logistics'], df_first['logistics_item'])
    last_map = get_item_company_map(df_last['project'], df_last['inventory'], df_last['inventory_item'], df_last['logistics'], df_last['logistics_item'])

    # 2. 평균 재고량 계산
    first_df = df_first['item'].merge(first_map, on="item_id", how="left").dropna(subset=["company_id"])
    last_df = df_last['item'].merge(last_map, on="item_id", how="left").dropna(subset=["company_id"])

    begin_inv = first_df.groupby("company_id")["item_quantity"].sum().reset_index(name="begin_inventory")
    end_inv = last_df.groupby("company_id")["item_quantity"].sum().reset_index(name="end_inventory")

    inv_summary = begin_inv.merge(end_inv, on="company_id", how="outer").fillna(0)
    inv_summary["avg_inventory"] = (inv_summary["begin_inventory"] + inv_summary["end_inventory"]) / 2

    # 3. 월간 출하량 계산
    # 당월 누적 출하량 - 월초 누적 출하량 = 당월 순수 출하량
    last_log_items = df_last['logistics_item'].merge(last_map, on="item_id", how="left").dropna(subset=["company_id"])
    first_log_items = df_first['logistics_item'].merge(first_map, on="item_id", how="left").dropna(subset=["company_id"])

    last_ship = last_log_items.groupby(["company_id", "logistics_item_id"])["logistics_processed_quantity"].max().reset_index(name="last_qty")
    first_ship = first_log_items.groupby(["company_id", "logistics_item_id"])["logistics_processed_quantity"].max().reset_index(name="first_qty")

    shipment = last_ship.merge(first_ship, on=["company_id", "logistics_item_id"], how="left").fillna(0)
    shipment["delta_ship"] = (shipment["last_qty"] - shipment["first_qty"]).clip(lower=0)

    total_ship = shipment.groupby("company_id")["delta_ship"].sum().reset_index(name="month_ship")

    # 4. 최종 재고 회전율 산출
    kpi = inv_summary.merge(total_ship, on="company_id", how="outer").fillna(0)
    kpi["inventory_turnover"] = np.where(
        kpi["avg_inventory"] == 0,
        0.0,
        kpi["month_ship"] / kpi["avg_inventory"]
    ).round(3)

    return kpi[["company_id", "inventory_turnover"]].to_dict(orient="records")