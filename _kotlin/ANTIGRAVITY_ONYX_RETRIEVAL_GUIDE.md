# Antigravity 작업 가이드 — Onyx 검색 동작 정리

## 목적

이 작업의 목적은 `onyx-kotlin`의 `main-v4.6.5` 브랜치를 **같은 작업 디렉토리에 존재하는 원본 Onyx Python 구현의 검색 동작과 가능한 한 동일하게 정리하는 것**이다.

현재 작업 환경에는 **원본 Onyx Python 소스가 함께 존재한다.** 따라서 외부 문서나 추측을 바탕으로 "비슷하게 재구현"하지 말고, 관련 기능의 원본 Python 구현을 직접 기준으로 삼아 Kotlin으로 포팅한다.

즉 이 작업의 기준은:

```text
원본 Onyx Python 구현
        ↓
동작 / 기본값 / score 계산 / edge case 확인
        ↓
Kotlin 구현에 그대로 반영
```

이다.

핵심은 아래 3가지이며, **이 3개가 최우선 요구사항**이다.

1. **Reranker 완전 제거**
2. **OpenSearch 자체 RRF를 사용하지 않고, 기존 Onyx 방식의 score normalization + hybrid score 조합 방식 유지/복원**
3. **Agentic Retrieval 성격의 검색 조합 로직을 MCP 계층에 넣어, 로컬 Claude Code가 검색 전략을 대리 수행할 수 있게 함**

그리고 위 작업이 끝난 뒤에만, backend에 남은 **명백한 미사용 소스(dead code)** 를 제한적으로 정리한다.

> 중요: 이 작업은 검색 아키텍처를 새로 설계하는 작업이 아니다.  
> 목적 달성에 필요한 최소 변경만 수행한다.  
> 과도한 리팩토링, 추상화 재설계, 신규 기능 추가, 대규모 패키지 이동은 금지한다.

---

## 작업 원칙

### 반드시 지킬 것

- 전체 레포를 처음부터 끝까지 읽지 않는다.
- 아래 키워드와 직접 연관된 경로만 좁혀서 추적한다.
  - `rerank`
  - `reranker`
  - `hybrid search`
  - `rrf`
  - `normalization`
  - `min-max`
  - `vector score`
  - `keyword score`
  - `bm25`
  - `mcp`
  - `search`
  - `retrieval`
- 기존 구현을 최대한 재사용한다.
- **원본 Onyx Python 소스가 같은 작업 디렉토리에 있으므로, 검색 관련 구현은 반드시 원본을 먼저 확인하고 포팅 기준으로 삼는다.**
- "Onyx와 비슷한 알고리즘"을 새로 설계하지 않는다. 원본 구현이 존재하면 **동작, 상수, 기본값, edge case까지 가능한 한 동일하게 가져온다.**
- 현재 동작을 바꾸는 경우, 반드시 변경 이유를 코드/커밋 단위로 설명 가능해야 한다.
- 제거 작업과 기능 변경 작업을 섞지 않는다.
- 테스트는 변경 범위에 필요한 수준으로만 추가/수정한다.
- 성능 최적화는 이번 작업 범위가 아니다.

### 하지 말 것

- "더 깔끔해 보인다"는 이유로 구조를 전면 변경하지 않는다.
- 새로운 검색 프레임워크를 도입하지 않는다.
- LangChain, LlamaIndex 같은 별도 orchestration framework를 끼워 넣지 않는다.
- 새로운 reranker를 대체 도입하지 않는다.
- OpenSearch native RRF로 단순 치환하지 않는다.
- backend 전체 dead-code cleanup을 수행하지 않는다.
- 검색과 무관한 DTO, API, connector, ingestion 구조를 건드리지 않는다.
- 필요 이상으로 interface/abstraction 계층을 늘리지 않는다.

---

# 0. 가장 중요한 기준: 원본 Onyx Python을 Source of Truth로 사용

현재 작업 디렉토리에는 Kotlin 포팅 대상뿐 아니라 **원본 Onyx Python 소스도 존재한다.**

따라서 이 작업에서 검색 동작의 Source of Truth는 외부 문서가 아니라 **동일 디렉토리의 원본 Python 구현**이다.

## 반드시 이렇게 작업할 것

검색 관련 기능별로 필요한 파일만 좁게 찾아서 다음을 비교한다.

```text
Original Onyx Python
        ↕
Current Kotlin implementation
```

확인 대상:

- hybrid query 구성
- BM25 / semantic subquery 구성
- candidate 개수
- score normalization 방식
- min-max 처리
- weight 기본값
- result merge 방식
- duplicate 처리
- filtering
- top-k 처리
- weighted RRF 상수와 계산 방식
- agentic retrieval용 query/result fusion
- 관련 edge case
- 관련 config / default value

원본에 구현이 존재하면 **새 알고리즘을 제안하거나 재설계하지 않는다.**

## 우선순위

```text
1. 원본 Python 코드의 실제 동작
2. 원본 Python 코드의 상수 / 설정값 / 주석
3. Onyx 공식 PR / 문서
4. 일반적인 검색 이론
```

일반적인 이론이 원본 구현과 다르더라도 이번 작업에서는 **원본 Onyx 동작을 우선**한다.

## 주의

"전체 Python 레포를 다 읽어라"는 뜻이 아니다.

반드시 필요한 검색 경로만 좁게 추적한다.

예:

```text
rerank
hybrid
normalization
min_max
search
weighted_rrf
agentic
mcp
```

관련 symbol/file만 찾아서 Kotlin 구현과 대응시킨다.


---

# 1. Reranker 완전 제거

## 목표

현재 backend와 model-server 사이에 존재하는 rerank 관련 호출 흐름을 모두 제거한다.

최종적으로 검색 경로에서 reranker가 **완전히 선택 사항이 아니라 아예 존재하지 않는 상태**가 되어야 한다.

## 제거 대상

다음 범주를 확인하고 제거한다.

### Backend

- rerank 요청 DTO
- rerank 응답 DTO
- reranker client
- reranker endpoint 호출
- reranker score 반영 로직
- reranker timeout/retry 설정
- reranker 관련 config / env
- reranker health check
- reranker용 feature flag
- reranker 결과를 전제로 한 분기
- reranker 관련 테스트
- reranker 전용 metric / log

### Model Server

- rerank endpoint
- reranker model load
- reranker inference
- reranker 관련 model config
- reranker 관련 health/status
- reranker 전용 dependency가 있다면 제거 가능 여부 확인

## 완료 조건

검색 요청 흐름이 다음과 같이 단순화되어야 한다.

```text
query
  -> keyword/vector retrieval
  -> score normalization / hybrid merge
  -> result selection
  -> response
```

아래 단계는 없어야 한다.

```text
-> reranker
-> reranked score
-> reranked ordering
```

## 주의

reranker 제거 후 품질 보정을 위해 임의의 heuristic을 추가하지 않는다.

이번 방향은 **retrieval 자체를 잘 구성하고, 후단 LLM/agent가 필요한 경우 추가 탐색을 수행하도록 하는 것**이다.

---

# 2. OpenSearch native RRF를 사용하지 않고 기존 Onyx식 score normalization 유지

## 핵심 방향

OpenSearch가 제공하는 native RRF 기능을 검색 fusion의 기본 구현으로 사용하지 않는다.

**같은 작업 디렉토리에 있는 원본 Onyx Python 코드의 실제 구현과 동일하게:**

```text
BM25 / keyword score
+
vector similarity score
    ↓
각 score를 normalization
    ↓
가중 합산 또는 기존 Onyx와 동일한 hybrid merge
    ↓
최종 candidate ranking
```

방식으로 유지한다.

---

## 왜 이 방향을 택하는가

Onyx는 단순히 "OpenSearch가 제공하는 최신 fusion API"를 호출하는 형태가 아니라, 검색 파이프라인 내부에서 **keyword / semantic score를 직접 다루는 방식**을 오랫동안 사용해 왔다.

이 접근의 장점은 다음과 같다.

- 검색엔진 implementation에 덜 종속적이다.
- BM25와 vector score scale 차이를 직접 보정할 수 있다.
- 검색 결과에 적용되는 weight를 애플리케이션에서 통제할 수 있다.
- 검색엔진 버전에 따라 RRF 동작이 달라지는 문제를 줄인다.
- 향후 OpenSearch 외 검색엔진으로 옮겨도 retrieval semantics를 유지하기 쉽다.
- agent가 검색 전략을 바꿔 여러 번 호출할 때 score 조합 정책을 애플리케이션 레이어에서 유지할 수 있다.

이 작업에서 중요한 것은 "RRF가 나쁘다"가 아니다.

**기존 Onyx가 실제 운영에서 사용해 온 retrieval behavior를 최대한 그대로 가져가는 것**이 목적이다.

---

## 구현 가이드

현재 코드에 이미 normalization / hybrid score 계산 코드가 있으면 그것을 우선 사용한다.

새로 구현해야 한다면 최소한 아래 형태를 따른다.

### Min-Max normalization 예시

```text
normalized_score =
    (score - min_score)
    / (max_score - min_score)
```

검색 결과 집합 내에서 keyword score와 vector score를 각각 동일한 범위로 normalize한 뒤 합산한다.

예:

```text
final_score =
    keyword_weight * normalized_keyword_score
    +
    vector_weight * normalized_vector_score
```

단, 위 공식은 개념 설명일 뿐이다. 실제 구현 시에는 **같은 작업 디렉토리의 원본 Onyx Python normalization / score merge 코드를 직접 확인하고 그 동작을 그대로 포팅한다.**

기존 Kotlin 구현이 원본 Python과 다르면, 특별한 이유가 없는 한 **Python 원본 기준으로 수정한다.**

---

## edge case

min == max인 경우 divide-by-zero가 발생하지 않도록 기존 Onyx 동작을 확인해 처리한다.

임의의 복잡한 보정식을 추가하지 않는다.

---

## OpenSearch native RRF 관련

다음과 같은 구현이 있으면 제거 또는 우회한다.

```text
OpenSearch RRF retriever
OpenSearch rank fusion API
OpenSearch RRF pipeline
```

최종 검색 결과는 애플리케이션 레이어에서 직접 조합해야 한다.

---

# 3. MCP에 Agentic Retrieval 지원 추가

## 목적

검색 엔진이 "질문 하나 -> 검색 한 번 -> 결과 반환"만 수행하는 구조를 넘어,

로컬 Claude Code가 필요에 따라:

- query를 여러 개로 나누고
- lexical / semantic 검색을 다르게 실행하고
- 결과를 여러 번 받아
- 최종 답변에 필요한 근거를 스스로 구성

할 수 있도록 한다.

중요한 점은 **backend 내부에 거대한 agent를 구현하는 것이 아니다.**

Agent 역할은 Claude Code가 한다.

MCP는 Claude Code가 검색 전략을 실행하기 위한 **retrieval primitive** 를 제공하는 계층으로 만든다.

---

## Onyx 방향성에서 가져올 것

Onyx의 최근 검색 방향에서 중요한 부분은 다음과 같다. 다만 아래 내용을 보고 새로 설계하지 말고, **원본 Python에 실제 구현된 agentic retrieval / search fusion 코드를 찾아 가능한 부분은 그대로 포팅한다.**

- 하나의 검색 결과만 blindly trust하지 않는다.
- 여러 retrieval path를 사용할 수 있다.
- lexical / semantic 결과를 각각 활용한다.
- query rewriting / decomposition을 상위 agent가 수행할 수 있다.
- 여러 retrieval 결과를 merge할 수 있다.
- rank fusion 시 단순 score sum뿐 아니라 rank 기반 조합도 사용할 수 있다.
- 필요하면 weighted RRF 형태로 서로 다른 검색 결과 집합을 조합할 수 있다.

---

# 4. MCP 검색 API 설계

현재 MCP API를 완전히 갈아엎지 않는다.

가능하면 기존 search tool을 확장하거나, 최소한의 별도 tool만 추가한다.

권장 primitive는 아래 정도다.

## 기본 검색

```text
search(
  query,
  search_type,
  top_k,
  filters
)
```

`search_type` 예:

```text
hybrid
keyword
semantic
```

---

## 여러 검색 결과 merge

필요하다면 MCP 측에서 다음 기능을 제공한다.

```text
merge_search_results(
  result_sets,
  weights,
  method
)
```

`method`:

```text
weighted_rrf
normalized_score
```

단, 이것을 위해 새로운 거대한 framework를 만들지 않는다.

---

# 5. Weighted RRF

Agentic Retrieval용 결과 조합에서는 weighted RRF를 사용할 수 있다.

일반적인 RRF 개념:

```text
score(d) =
Σ 1 / (k + rank_i(d))
```

weighted RRF:

```text
score(d) =
Σ weight_i / (k + rank_i(d))
```

예:

```text
semantic search weight = 1.0
keyword search weight  = 0.7
```

이 방식의 장점은 서로 다른 retrieval source의 raw score scale이 달라도 **rank만으로 결과를 합칠 수 있다는 점**이다.

---

## 중요한 구분

### 기본 검색 pipeline

```text
keyword score
+
vector score
+
normalization
+
weighted merge
```

### Agentic Retrieval에서 여러 검색 실행 결과를 다시 합치는 경우

```text
search result A
search result B
search result C
    ↓
weighted RRF
```

즉, weighted RRF는 OpenSearch native RRF를 기본 hybrid search에 쓰자는 뜻이 아니다.

**서로 다른 agentic retrieval 실행 결과를 상위 계층에서 합치기 위한 도구**로 사용한다.

---

# 6. Claude Code가 담당할 역할

Claude Code가 다음을 판단할 수 있어야 한다.

예:

```text
사용자 질문
↓
질문 분석
↓
필요한 검색 전략 결정

1. 정확한 이름 / ID / 에러 메시지
   -> keyword search

2. 개념 / 유사 의미
   -> semantic search

3. 복합 질문
   -> query decomposition

4. 결과가 부족함
   -> query rewrite 후 재검색

5. 여러 결과 획득
   -> weighted RRF 또는 자체 판단
```

즉, MCP는 "검색을 대신 생각하는 agent"가 아니라

```text
Claude Code가 검색 전략을 실행할 수 있는 retrieval API
```

역할이어야 한다.

---

# 7. 구현 범위 예시

예를 들어 아래 정도면 충분하다.

```text
MCP
 ├─ search_hybrid()
 ├─ search_keyword()
 ├─ search_semantic()
 └─ optional: fuse_results()
```

Claude Code:

```text
question
   ↓
query decomposition
   ↓
search_keyword()
search_semantic()
search_hybrid()
   ↓
필요시 추가 검색
   ↓
최종 답변
```

별도의 agent framework는 필요 없다.

---

# 8. Backend dead code 정리

위 3개 핵심 작업이 완료된 다음에만 수행한다.

## 정리 대상

이번 변경으로 인해 명백하게 사용되지 않게 된 코드만 삭제한다.

예:

- reranker DTO
- reranker client
- reranker config
- reranker service
- reranker metric
- reranker test fixture
- OpenSearch native RRF wrapper
- 더 이상 호출되지 않는 fusion adapter

## 삭제 금지

다음은 이번 작업에서 건드리지 않는다.

- connector
- ingestion
- sync
- auth
- admin
- document processing
- unrelated model-server code
- unrelated search API

---

# 9. 작업 순서

아래 순서대로 진행한다.

## Step 1

현재 검색 요청 흐름만 좁게 추적한다.

동시에 **같은 작업 디렉토리의 원본 Onyx Python에서 대응되는 검색 흐름을 찾아 1:1로 매핑한다.**

예:

```text
Python symbol/file          Kotlin symbol/file
------------------          ------------------
hybrid search        <->    hybrid search
normalization        <->    normalization
weighted RRF         <->    MCP retrieval fusion
reranker             <->    reranker
```

전체 소스를 읽지 않고 관련 symbol 기준으로만 확인한다.

```text
MCP
-> backend search API
-> OpenSearch
-> model-server
```

전체 코드를 읽지 않는다.

---

## Step 2

reranker 호출을 제거한다.

backend와 model-server 모두 확인한다.

---

## Step 3

현재 hybrid search score merge 방식을 확인한다.

OpenSearch RRF를 사용 중이면 제거한다.

기존 Onyx식 normalization / weighted score merge가 있으면 그것을 유지/복원한다.

---

## Step 4

MCP search tool을 agent-friendly primitive 형태로 정리한다.

최소한:

```text
keyword
semantic
hybrid
```

를 구분해 호출 가능하도록 한다.

---

## Step 5

필요하면 weighted RRF merge helper를 추가한다.

단, agentic search result merge 용도로만 사용한다.

---

## Step 6

Claude Code에서 아래 패턴이 가능한지 테스트한다.

```text
질문
-> keyword 검색
-> semantic 검색
-> 필요시 query 변경
-> 재검색
-> 결과 취합
```

---

## Step 7

이번 변경으로 발생한 dead code만 삭제한다.

---

# 10. 테스트 요구사항

과도한 테스트를 만들지 않는다.

최소한 아래만 보장한다.

### reranker 제거

- 검색 중 reranker endpoint가 호출되지 않는다.
- reranker config 없이 backend/model-server가 정상 기동한다.

### hybrid search

동일한 candidate set에 대해:

```text
keyword score normalization
vector score normalization
final score merge
```

가 정상 동작한다.

### MCP

각각 정상 호출 가능한지 확인한다.

```text
keyword search
semantic search
hybrid search
```

### weighted RRF

간단한 fixture 2~3개로 rank merge 결과가 기대 순서와 일치하는지만 확인한다.

---

# 11. 완료 기준

다음 조건을 모두 만족해야 한다.

- [ ] backend에 reranker 호출이 없다.
- [ ] model-server에 reranker endpoint / inference가 없다.
- [ ] reranker 관련 config가 제거되었다.
- [ ] OpenSearch native RRF를 기본 hybrid retrieval에 사용하지 않는다.
- [ ] keyword / vector score normalization 기반 hybrid merge가 동작한다.
- [ ] MCP에서 keyword / semantic / hybrid 검색을 구분해 호출할 수 있다.
- [ ] Claude Code가 여러 retrieval을 조합할 수 있다.
- [ ] 필요한 경우 weighted RRF로 여러 검색 결과 집합을 합칠 수 있다.
- [ ] 이번 변경 때문에 생긴 dead code만 제거했다.
- [ ] 검색과 무관한 구조는 변경하지 않았다.

---

# 12. 가장 중요한 판단 기준

구현 중 애매한 선택지가 생기면 아래 순서로 판단한다.

```text
1. 원본 Onyx Python 구현과 동일한가?
2. 현재 코드를 가장 적게 바꾸는가?
3. 검색엔진 구현에 불필요하게 종속되지 않는가?
4. Claude Code가 MCP를 통해 검색 전략을 직접 수행할 수 있는가?
5. 신규 abstraction이나 framework를 추가하지 않고 해결 가능한가?
```

이 기준에서 벗어나는 변경은 하지 않는다.

---

# 참고 방향

이번 작업에서 참고해야 할 Onyx의 핵심 설계 철학은 다음과 같다.

```text
Retrieval
  ≠ 단순 vector search

Retrieval
  = lexical retrieval
  + semantic retrieval
  + score normalization
  + result fusion
  + 필요 시 multi-query / agentic retrieval
```

또한 reranker는 필수 요소가 아니다.

특히 외부 agent가 retrieval을 반복 수행하고 결과를 판단하는 구조에서는:

```text
강한 retrieval primitive
+
agentic query planning
```

만으로도 충분히 좋은 구조를 만들 수 있다.

이번 프로젝트는 정확히 이 방향으로 단순화한다.

그리고 이 방향을 새로 해석해서 구현하지 않는다. **현재 작업 디렉토리에 있는 원본 Onyx Python 구현을 직접 참조해서 동일하게 가져오는 것이 기본 원칙이다.**

---

# 최종 지시

**핵심 요구사항은 아래 3개다. 다른 개선보다 무조건 우선한다.**

```text
1. reranker 제거
2. OpenSearch native RRF 대신 **원본 Onyx Python과 동일한** normalization 기반 hybrid scoring
3. MCP에 **원본 Onyx의 agentic retrieval / weighted RRF 구현을 최대한 그대로 포팅한** 검색 primitive
```

그리고 작업 후 **이번 변경으로 인해 남은 backend 미사용 코드만** 정리한다.

그 외 리팩토링이나 신규 기능 개발은 하지 않는다.
