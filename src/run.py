import pandas as pd
import boto3
import json
from datetime import datetime
from item_kpi import calculate_item_kpi_by_company
from config import S3_BUCKET, RAW_PREFIX, KPI_PREFIX, AWS_REGION

s3_client = boto3.client("s3", region_name=AWS_REGION)

def get_latest_raw_file():
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=RAW_PREFIX
    )

    objects = response.get("Contents", [])
    if not objects:
        raise RuntimeError(f"No raw files found in bucket: {S3_BUCKET} with prefix: {RAW_PREFIX}")

    # 최신 파일 찾기
    latest = max(objects, key = lambda x: x["LastModified"])
    return latest["Key"]
def run():
    # 1. 최신 RAW CSV 가져오기
    raw_key = get_latest_raw_file()
    print(f"Using raw file: {raw_key}")

    # 2. boto3를 사용하여 S3 오브젝트를 직접 가져옴
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=raw_key)
    # response['Body']를 pandas에 직접 전달합니다.
    df = pd.read_csv(response['Body'])

    # 3. 회사별 KPI 계산
    kpi_results = calculate_item_kpi_by_company(df)

    today = datetime.now().strftime("%Y-%m-%d")
    month = today[:7]

    # 회사별로 S3 저장
    for kpi in kpi_results:
        company_id = kpi["companyId"]

        payload = {
            "companyId": company_id,
            "kpiType": "SAFETY_STOCK_RATE",
            "snapshotMonth": month,
            "calculatedAt": datetime.now().isoformat(),
            "data": {
                "rate": kpi["safetyStockRate"],
                "totalItems": kpi["totalItems"],
                "securedItems": kpi["securedItems"]
            }
        }

        key = (
            f"{KPI_PREFIX}/inventory/"
            f"company-{company_id}/"
            f"safety_stock_{month}.json"
        )

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False),
            ContentType="application/json"
        )

        print(f"KPI uploaded -> {key}")

# --- 람다 핸들러 추가 ---
def lambda_handler(event, context):
    # AWS 람다 엔트리 포인트
    try:
        run()
        return {
            'statusCode': 200,
            'body': json.dumps('ETL Job successfully completed')
        }
    except Exception as e:
        # 에러 발생 시 CloudWatch Logs에 기록되도록 출력
        print(f"ETL failed: {str(e)}")
        raise e

# 로컬 실행 용도
if __name__ == "__main__":
    run()