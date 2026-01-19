## 목차

1. [**웹 서비스 소개**](#1)
1. [**SW 아키텍처**](#2)
1. [**기술 스택**](#3)
1. [**주요 기능**](#4)
1. [**폴더 구조**](#5)
1. [**Git 협업 규칙**](#6)

---
<div id="1"></div>

# NexERP Etl

<img width="957" height="530" alt="erp" src="https://github.com/user-attachments/assets/d630f329-431f-4334-8a40-4a1c190d0bc2" />


## 프로젝트 개요

<blockquote>
NexERP는 NextGen(차세대)과 ERP(전사적 자원 관리)를 결합한 서비스로 단순한 관리를 넘어 내일의 성장을 주도하는 클라우드 솔루션을 제공합니다.
</blockquote>

#### 👥 프로젝트 백엔드 팀원

|    _이름_    |                                                                 윤민섭                                                                  |                                                                  이은희                                                                  |                                                                 정윤주                                                                 |
|:----------:|:------------------------------------------------------------------------------------------------------------------------------------:|:-------------------------------------------------------------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------------------------------------------------:|
| _역할(Role)_ |                                                               BE, INFRA                                                                |                                                               DA                                                               |                                                               DA                                                                |
|            | <a href="https://github.com/minsubyun1"><img src="https://avatars.githubusercontent.com/u/75060858?v=4" width="128" height="128"></a> | <a href="https://github.com/e-unhee"><img src="https://avatars.githubusercontent.com/u/203168745?v=4" width="128" height="128"></a> | <a href="https://github.com/jyunju92"><img src="https://avatars.githubusercontent.com/u/175554725?v=4" width="128" height="128"></a> |

---

<div id="2"></div>

## SW 아키텍처



### 데이터 및 ETL 파이프라인 (Data Pipeline)
<img width="685" height="262" alt="image" src="https://github.com/user-attachments/assets/0f4d3152-6a3d-47aa-8b01-b2139d024db3" />

NexERP의 핵심인 예측 KPI를 생성하기 위한 배치 처리 프로세스입니다.

- 운영 환경의 부하를 최소화
    - RDS Read Replica 활용: 매일 새벽 진행되는 대규모 데이터 추출 작업이 실제 사용자의 서비스 이용(OLTP)에 영향을 주지 않도록, 운영 DB가 아닌 읽기
      복제본에서 데이터를 추출
    - 성능 격리: 이를 통해 분석 쿼리로 인한 운영 서버의 CPU/Memory 점유율 상승 차단


- 서비리스 기반의 ETL 파이프라인
    - Step 1. Data Loading (02:00 KST): 복제본에서 추출된 로우 데이터(Raw Data)를 CSV 형태로 S3 Data Lake에 적재
    - Step 2. Analysis & Schema Conversion (03:00 KST): Event Bridge 스케줄러가 AWS Lambda를 트리거하여, S3의
      데이터를 분석하고 서비스 규격에 맞는 JSON 형태로 변환
    - Step 3. Persistent Snapshot (04:00 KST): 분석 결과를 다시 RDS 테이블에 저장


- 스토리지 및 비용 최적화: S3에 저장된 원본 데이터는 LifeCycle Policy에 의해 120일 후 자동 삭제하여 스토리지 비용 관리 효율 확보

---
<div id="3"></div>

## 기술 스택

### 개발 & ML 라이브러리

[![My Skills](https://skillicons.dev/icons?i=python,aws&perline=3)](https://skillicons.dev)

![XGBoost](https://img.shields.io/badge/XGBoost-ML-informational)
![statsmodels](https://img.shields.io/badge/XGBoost-ML-informational)

### 협업 도구

[![My Skills](https://skillicons.dev/icons?i=notion&perline=3)](https://skillicons.dev)

---
<div id="4"></div>

## 주요 기능


### KPI 대시보드

매일 자동 수집 -> 분석 -> S3 저장 -> DB 저장 과정 자동화

- 프로젝트 처리 완료율
- 업무 장기 처리율
- 안전재고 확보율
- 재고 회전율
- 출하 완료율
- 출하 리드타임
- 예측 KPI (재고 회전율, 출하 리드타임)

[스웨거 링크](https://nexerp.site/swagger-ui/index.html)

---
<div id="5"></div>

### 📁 폴더 구조

```
nexerp
└─ src
   ├─ analytics
   └─ init_analysis
     
                      
```

---

<div id="6"></div>

## 🤝 Git 협업 규칙

### 이슈 타입 분류

| 타입             | 설명                    | 
|:---------------|:----------------------| 
| **[Feature]**  | **새로운 기능 추가**         | 
| **[Refactor]** | **기능 변화 없는 코드 구조 개선** | 
| **[Proposal]** | **협업 개선**             | 
| **[Bug]**      | **발견된 버그 수정 사항**      | 
| **[Help]**     | **도움 요청**             |

### 커밋 메시지 컨벤션

#### 커밋 메시지 구조

```
<타입>: <제목>

본문 내용 (선택)
```

- 커밋 메시지는 한글로 작성합니다.
- 커밋은 하나의 작업 단위로 구분됩니다.

#### 커밋 메시지 유형

| **유형**               | **설명**                         | 
|:---------------------|:-------------------------------| 
| **feature**          | **새로운 기능 추가**                  | 
| **refactor**         | **코드 리팩토링 (기능 변화 없이 구조 개선)**   | 
| **fix**              | **일반적인 버그 수정**                 | 
| **docs**             | **문서 수정 (README, 주석, 위키 등)**   | 
| **test**             | **테스트 코드 추가**                  |
| **style**            | **코드 포맷팅 (공백, 세미콜론 등 스타일 변경)** | 
| **chore**            | **기타 관리 작업 (라이브러리 업데이트 등)**    | 
| **comment**          | **주석 추가 및 변경**                 | 
| **rename**           | **파일/폴더명 수정 및 위치 이동**          | 
| **remove**           | **파일 삭제**                      | 
| **!BREAKING CHANGE** | **큰 API 변경**                   | 
| **fix**              | **버그 수정**                      | 
| **!HOTFIX**          | **긴급한 버그 수정**                  | 

#### 커밋 메시지 예시

```
feature: 로그인 API 구현
refactor: UserService 로직 분리
```
