import pandas as pd
from pathlib import Path

# 기본 파일 경로
project_csv= "project--2026-01-31.csv"
logistics_csv = "logistics--2026-01-31.csv"
logistics_item_csv = "logistics_item--2026-01-31.csv"
inventory_item_csv = "inventory_item--2026-01-31.csv"
inventory_csv = "inventory--2026-01-31.csv"
item_csv = "item--2026-01-31.csv"

fix_cols = {
    "project": [
        "date",
        "project_id",
        "company_id",
        "project_status",
        "project_create_date",
        "project_end_date",
        "project_expected_end_date",
    ],
    "inventory": [
        "date",
        "inventory_id",
        "project_id",
        "inventory_create_at",
        "inventory_status",
        "inventory_completed_at",
    ],
    "logistics": [
        "date",
        "logistics_id",
        "project_id",
        "logistic_create_at",
        "logistics_status",
        "logistics_completed_at",
    ],
    "inventory_item": [
        "date",
        "inventory_item_id",
        "item_id",
        "inventory_id",
    ],
    "logistics_item": [
        "date",
        "logistics_item_id",
        "item_id",
        "logistics_id",
        "logistics_processed_quantity",
    ],
    "item": [
        "date",
        "item_id",
        "item_quantity",
        "safety_stock",
    ],
}

# 어차피 월말 스냅샷으로만 진행 -> 날짜 확인 로직 제거

# CSV에서 필요한 컬럼만 읽어오는 함수
def read_csv(
    csv: str,
    required_cols: list[str],
    name: str
) -> pd.DataFrame:
    
    p = Path(csv)
    if not p.exists():
        raise FileNotFoundError(f"CSV 파일이 없습니다: {csv}")

    df = pd.read_csv(p)
    csv_cols = set(df.columns)

    # 필수 컬럼 체크
    missing_cols = set(required_cols) - csv_cols
    if missing_cols:
        raise ValueError(f"Missing columns: {sorted(missing_cols)}")

    return df[required_cols]