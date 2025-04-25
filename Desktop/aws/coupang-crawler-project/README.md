# Coupang Crawler Project

쿠팡 웹사이트 상품 정보를 자동으로 수집하는 크롤러 프로젝트입니다.  
설정, 상태 저장, 셀렉터 관리 등 모든 기능이 구조적으로 잘 분리되어 있어,  
누구나 손쉽게 사용·확장할 수 있습니다.

---

## 📁 폴더 구조

```
coupang-crawler-project/
├── config/
│   ├── crawler_config.json      # 크롤러 동작 설정
│   ├── crawler_status.json      # 크롤링 진행 상태
│   └── selectors.json           # 사이트 셀렉터 정보
├── logs/                        # 크롤링 실행 로그
├── RAW/                         # (원본 데이터 등)
├── coupang_merged.xlsx          # (병합된 결과 예시)
├── crawler.py                   # 메인 크롤러 실행 파일
├── merger.py                    # 결과 병합 등 유틸
├── requirements.txt             # 필요한 파이썬 패키지
├── source_urls.xlsx             # 크롤링 대상 URL 목록 예시
└── venv/                        # (가상환경)
```

---

## 🚀 설치 및 실행 방법

### 1. 가상환경 준비

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Mac/Linux
source venv/bin/activate
```

### 2. 필요 패키지 설치

```bash
pip install -r requirements.txt
```

### 3. config 파일 설정

- `config/crawler_config.json` : 크롤러 동작 옵션(대기시간, 브라우저 등)
- `config/selectors.json` : 크롤링 대상 사이트 셀렉터
- `config/crawler_status.json` : (자동 생성/업데이트됨)

### 4. 크롤러 실행

```bash
python crawler.py --file [엑셀 파일 경로] [옵션]
```

#### 주요 옵션 예시

- `--file source_urls.xlsx` : 크롤링할 엑셀 파일 지정(필수)
- `--batch 15` : 배치 단위 크롤링
- `--start 100 --end 200` : 특정 범위만 크롤링
- `--no-restart` : 자동 재시작 비활성화
- `--no-validate` : 결과 검증 비활성화
- `--output my_results.xlsx` : 결과 파일명 지정
- `--config` : 현재 설정 확인

#### 사용 예시

```bash
python crawler.py --file source_urls.xlsx --batch 15 --output result.xlsx
```

---

## 🛠️ 주요 기능

- **설정 파일 분리** : config 폴더 내 json 파일로 모든 옵션/상태/셀렉터 관리
- **자동 상태 저장/복구** : 중단 후 이어서 크롤링 가능
- **로깅** : logs 폴더에 상세 실행 로그 저장
- **확장성** : 셀렉터/설정/입력파일만 바꿔 다양한 크롤링에 활용 가능

---

## ⚠️ 참고/유의사항

- 크롤러 실행 위치와 상관없이 항상 올바른 config 파일을 불러옵니다.
- 크롤링 대상 엑셀 파일은 반드시 `--file` 옵션으로 지정해야 합니다.
- 셀렉터/설정 파일은 프로젝트 구조에 맞게 수정하세요.
- 크롤링 도중 오류가 발생하면, logs 폴더의 로그 파일을 확인하세요.

---

## 👨‍💻 개발/기여

- 추가 기능, 버그 제보, 코드 개선은 언제든 환영합니다!
- 코드 구조와 설정 파일만 맞추면 누구나 쉽게 확장 가능합니다.

---
