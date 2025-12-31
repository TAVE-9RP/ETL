import pandas as pd

def calculate_item_kpi_by_company(df: pd.DataFrame) -> list[dict]:
    """

    companyId 기준으로 안전재고 확보율 KPI 계산
    """
    results = []

    for company_id, group in df.groupby("companyId"):
        total_items = len(group)

        if total_items == 0:
            rate = 0.0
            secured_items = 0
        else:
            secured_items = (group["quantity"] >= group["safetyStock"]).sum()
            rate = round((secured_items / total_items) * 100, 2)

        results.append({
            "companyId": int(company_id),
            "safetyStockRate": rate,
            "totalItems": int(total_items),
            "securedItems": int(secured_items)
        })

    return results