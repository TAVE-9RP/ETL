# 1. AWS 람다용 파이썬 3.11 베이스 이미지
FROM public.ecr.aws/lambda/python:3.11

# 2. 필수 시스템 라이브러리 설치 (컴파일러 대신 런타임 라이브러리 중심)
# libgomp는 XGBoost 실행에 반드시 필요한 OpenMP 라이브러리
RUN yum install -y gcc gcc-c++ python3-devel cmake make libgomp && yum clean all

# 3. pip 및 빌드 도구 최신화
RUN pip install --upgrade pip setuptools wheel

# 4. 의존성 설치 (컴파일을 건너뛰고 바이너리만 설치하도록 강제)
COPY requirements.txt .
# --only-binary=:all: 소스 컴파일 원천 차단 옵션
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.txt

# 5. 운영 코드 복사
COPY src/analytics/ .

# 6. 람다 핸들러 설정
CMD ["run.lambda_handler"]