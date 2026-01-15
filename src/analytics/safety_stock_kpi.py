import pandas as pd
import numpy as np

def calculate_safety_stock_rate(df_project: pd.DataFrame, df_inventory: pd.DataFrame, df_inventory_item: pd.DataFrame, df_logistics: pd.DataFrame, df_logistics_item: pd.DataFrame, df_item: pd.DataFrame):
    """
    안전재고 확보율 계산:
    입고(Inventory)와 출하(Logistics) 경로를 모두 추적하여 품목별 회사/프로젝트 매핑을 수행
    """

    # 가독성을 위해 컬럼 추출을 따로 빼지 않고, 경로 매핑에 포함
    # 1. 입고 경로 매핑 (Item -> Inventory_Item -> Inventory -> Project)
    inv_map = (
        df_inventory_item.merge(df_inventory, on="inventory_id", how="left")
        .merge(df_project, on="project_id", how="left")
        .dropna(subset=["company_id", "project_id"])
        .drop_duplicates(subset=["item_id"], keep="last")[["item_id", "company_id", "project_id"]]
    )

    # 2. 출하 경로 매핑 (Item -> Logistics_Item -> Logistics -> Project)
    log_map = (
        df_logistics_item.merge(df_logistics, on="logistics_id", how="left")
        .merge(df_project, on="project_id", how="left")
        .dropna(subset=["company_id", "project_id"])
        .drop_duplicates(subset=["item_id"], keep="last")[["item_id", "company_id", "project_id"]]
    )

    # 3. 입고와 출하 데이터 통합 (어느 한쪽이라도 정보가 있으면 활용)
    item_map = inv_map.merge(log_map, on="item_id", how="outer", suffixes=("_inv", "_log"))

    # 입고 정보가 우선, 없으면 출하 정보 사용 (combine_first)
    item_map["company_id"] = item_map["company_id_inv"].combine_first(item_map["company_id_log"])
    item_map["project_id"] = item_map["project_id_inv"].combine_first(item_map["project_id_log"])
    item_map = item_map[["item_id", "company_id", "project_id"]]

    # 4. 마스터 품목 리스트에 매핑 정보 결합
    df = df_item.merge(item_map, on="item_id", how="left")

    # 5. 타입 보정 및 결측치 처리
    df["item_quantity"] = pd.to_numeric(df["item_quantity"], errors="coerce").fillna(0)
    df["safety_stock"] = pd.to_numeric(df["safety_stock"], errors="coerce").fillna(0)

    # 6. 분석 대상 필터링 (회사와 프로젝트가 확인된 품목만)
    kpi_base = df.dropna(subset=["company_id", "project_id"]).copy()

    # 7. 아이템 단위로 그레인 맞추기 (최신 상태 유지)
    item_level = (
        kpi_base.sort_values(["company_id", "item_id"])
        .drop_duplicates(subset=["company_id", "item_id"], keep="last")
        .copy()
    )

    # 8. 안전재고 확보 여부 파악 (secured_flag)
    item_level["secured_flag"] = (item_level["item_quantity"] >= item_level["safety_stock"]).astype(int)

    # 9. 회사별 집계
    kpi = (
        item_level.groupby("company_id", as_index=False)
            .agg(
                total_items=("item_id", "nunique"),
                secured_items=("secured_flag", "sum")
        )
    )

    # 10. 안전재고 확보율 계산
    kpi["safety_stock_rate_monthly"] = np.where(
        kpi["total_items"] == 0,
        np.nan,
        (kpi["secured_items"] / kpi["total_items"] * 100.0)
    ).round(3)

    return kpi.to_dict(orient="records")