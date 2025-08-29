# 프롬프트 관리 시스템

## 📋 개요

모든 API 호출에 사용되는 프롬프트를 중앙에서 관리하는 시스템입니다.
**`utils/prompts.py`** 파일 하나만 수정하면 전체 시스템에 적용됩니다.

## 🏗️ 구조

```
utils/prompts.py
├─ DataCollectionPrompts    # 데이터 수집용 프롬프트
├─ APIPrompts              # API 시스템 프롬프트  
└─ ArtistPrompts           # 하위 호환성용 (기존 코드)
```

## 📊 테이블별 프롬프트 매핑

| 테이블/컬럼 | 프롬프트 함수 | 용도 |
|-------------|---------------|------|
| **artists.csv** | | |
| └─ 모든 컬럼 | `get_artist_info_prompt()` | 아티스트 정보 수집 |
| **concerts.csv** | | |  
| ├─ artist | `get_artist_name_prompt()` | 아티스트명 추출 |
| ├─ artist | `get_artist_display_prompt()` | 아티스트명 보정 |
| └─ ticket_url | `get_ticket_link_prompt()` | 예매 링크 수집 |
| **setlists.csv** | | |
| └─ 모든 컬럼 | `get_expected_setlist_prompt()` | 예상 셋리스트 |
| └─ 모든 컬럼 | `get_actual_setlist_prompt()` | 실제 셋리스트 |
| **cultures.csv** | | |
| └─ 모든 컬럼 | `get_culture_info_prompt()` | 문화 정보 |
| **schedule.csv** | | |
| └─ 모든 컬럼 | `get_schedule_info_prompt()` | 일정 정보 |
| **md.csv** | | |
| └─ 모든 컬럼 | `get_merchandise_prompt()` | 굿즈 정보 |

## 🔧 사용 방법

### 1. 프롬프트 수정
```python
# utils/prompts.py에서 수정
@staticmethod
def get_artist_info_prompt(artist_name: str) -> str:
    return f"""수정된 프롬프트 내용..."""
```

### 2. 코드에서 사용
```python
from utils.prompts import DataCollectionPrompts

# 아티스트 정보 수집
prompt = DataCollectionPrompts.get_artist_info_prompt("IU (아이유)")
response = api.query_with_search(prompt)
```

## 📁 사용되는 파일

- **data_processing/enhanced_data_collector.py** - 메인 데이터 수집
- **src/gemini_api.py** - Gemini API 시스템 프롬프트  
- **src/perplexity_api.py** - Perplexity API 시스템 프롬프트

## 🔄 마이그레이션 완료 상태

- ✅ `utils/artist_prompts.py` → `utils/prompts.py`로 통합
- ✅ `data_processing/data_enhancement.py` 제거 (미사용)
- ✅ `enhanced_data_collector.py` 업데이트
- ✅ 하위 호환성 유지 (`ArtistPrompts` 별칭 제공)

## 💡 장점

1. **단일 진실의 원천**: 프롬프트가 한 곳에서만 관리됨
2. **쉬운 수정**: 프롬프트 변경이 필요할 때 한 파일만 수정
3. **일관성 보장**: 모든 곳에서 동일한 프롬프트 사용
4. **명확한 문서화**: 각 프롬프트의 용도와 위치가 명확함