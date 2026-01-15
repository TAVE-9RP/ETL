import os

try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Environment variables loaded from .env file")
except ImportError:
    # 람다 환경에서는 여기로 들어옴.
    print("python-dotenv not found. Using system environment variables.")

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "ap_southeast-2")
RAW_PREFIX = os.getenv("RAW_PREFIX", "exports/daily/")
KPI_PREFIX = os.getenv("KPI_PREFIX", "kpi")

if not S3_BUCKET:
    print("경고: S3_BUCKET 환경 변수를 로드하지 못했습니다!")