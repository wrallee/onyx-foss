# Kotlin 검색 MCP 설계

## 목적

Kotlin 백엔드에 하나의 검색 공통 서비스를 추가합니다. MCP의 `search`
도구는 이 서비스를 직접 호출합니다. 이후 REST API도 같은 서비스를 사용할
수 있습니다.

첫 버전은 인증 없이 Web의 공개 주소를 사용하는 배포를 대상으로 합니다.
Spring Boot 업그레이드와 Spring AI 도입은 `WL-20260902-001`에서 별도로
검토합니다.

## 현재 상태

- `ModelServerClient`는 문서 적재용 passage embedding만 생성합니다.
- `OpenSearchIndexer`는 문서 쓰기와 관리 작업만 수행합니다.
- `RerankingService`는 호출자가 제공한 후보만 model-server로 전달합니다.
- 전체 검색 흐름과 MCP 서버는 없습니다.
- Web의 catch-all API route는 요청 본문과 streaming 응답을 Kotlin 백엔드로
  전달할 수 있습니다.

## 구현 범위

첫 버전은 다음 기능만 제공합니다.

1. `search(query, document_sets, limit)` MCP 도구
2. 검색어 embedding 생성
3. OpenSearch keyword 후보와 vector 후보 검색
4. 후보 병합과 중복 제거
5. 기존 reranker를 사용한 최종 정렬
6. 제목, 본문 chunk, 링크, 검색 점수를 포함한 구조화 결과
7. 여러 Document Set 이름을 합집합으로 적용하는 선택 필터
8. Web의 `/mcp` 경로를 통한 backend `/mcp` 전달

다음 기능은 포함하지 않습니다.

- 사용자 인증과 문서 ACL 필터
- connector 필터
- 서버 내부 LLM 또는 agentic loop
- 검색 세션 상태
- Spring AI와 Spring AI Vector Store
- 별도 REST 검색 엔드포인트
- 전문 조회용 `fetch` MCP 도구

## 선택한 구조

```text
MCP search(query, document_sets?, limit)
        |
        v
SearchService
        |-- ModelServerClient.embedQuery()
        |-- OpenSearchIndexer.searchCandidates()
        `-- RerankingService.rerank()
```

MCP 계층은 protocol 변환만 담당합니다. 검색 정책은 `SearchService`에 둡니다.
MCP가 backend의 REST endpoint를 다시 호출하지 않습니다.

## 검색 흐름

1. `SearchService`는 공백이 아닌 query와 `1..20` 범위의 limit을 받습니다.
2. `document_sets`가 있으면 DB에서 이름을 확인합니다. 없는 이름은 tool
   execution error로 반환합니다.
3. `ModelServerClient`는 `text_type=query`로 embedding 하나를 생성합니다.
4. `OpenSearchIndexer`는 keyword 검색과 k-NN 검색을 각각 수행합니다.
5. 각 검색은 최대 50개의 후보를 가져옵니다.
6. Document Set 이름은 두 검색에 같은 `terms` 필터로 적용합니다. 여러 이름
   중 하나라도 포함한 chunk를 검색하므로 결과는 합집합입니다.
7. 서비스는 `(source_document_id, chunk_id)`를 후보 식별자로 사용합니다.
8. 서비스는 두 결과의 reciprocal-rank 점수를 더해 후보를 병합합니다.
9. 서비스는 합산 점수가 높은 후보 30개를 reranker에 전달합니다.
10. reranker가 성공하면 rerank 점수 순서로 limit개를 반환합니다.
11. reranker가 실패하면 reciprocal-rank 순서로 limit개를 반환합니다.

첫 버전은 두 검색을 별도로 수행합니다. OpenSearch search pipeline을 추가하지
않습니다. 이 방식은 설정과 운영 상태를 늘리지 않으면서 keyword와 vector
후보를 모두 사용합니다.

검색 후보 수는 `ONYX_SEARCH_CANDIDATES`로 설정하며 기본값은 50입니다.
리랭킹 후보 수는 `ONYX_SEARCH_RERANK_CANDIDATES`로 설정하며 기본값은
30입니다. 리랭킹 후보 수는 결과 limit 이상이고 `rerankerMaxDocuments` 이하로
제한합니다. 현재 `RERANKER_MAX_DOCUMENTS=100`은 요청 안전 상한으로 유지하며,
일반 검색이 100개를 모두 리랭킹하지는 않습니다.

## OpenSearch index

`embedding` 필드를 다음 값으로 명시합니다.

- type: `knn_vector`
- dimension: `768`
- engine: `lucene`
- space type: `cosinesimil`
- method: `hnsw`

인덱스 설정에서 `index.knn=true`를 사용합니다. Embedding 차원은
`ONYX_EMBEDDING_DIMENSION`으로 설정하며 기본값은 현재 Granite 모델의 768입니다.

기존 인덱스에서 `embedding`이 동적 숫자 배열로 생성됐으면 mapping을 직접
변경할 수 없습니다. 이 시스템은 아직 구축 단계이므로 기존 embedding을
migration하지 않습니다. 사용자가 배포 전에 해당 Onyx index를 명시적으로
삭제하고 모든 connector를 다시 실행해 새 mapping으로 적재합니다.

애플리케이션은 기존 `embedding` mapping이 `knn_vector`가 아니면 검색과 쓰기를
실패시킵니다. 애플리케이션과 이번 구현 작업은 index를 삭제하지 않습니다.

## MCP protocol

MCP Java SDK `2.0.1`의 core Servlet Streamable HTTP transport를 사용합니다.
Spring AI dependency는 추가하지 않습니다. Jackson 2 adapter를 사용해 현재
Spring Boot 3.4.5 JSON stack과 연결합니다.

서버는 backend `/mcp`에 stateless Streamable HTTP endpoint를 제공합니다.
Web은 `/mcp`를 기존 `/api/mcp` catch-all proxy로 rewrite합니다. 요청의 MCP
headers와 streaming response를 그대로 전달합니다.

MCP client는 다음 원격 URL만 등록하면 됩니다.

```text
https://onyx-admin.com/mcp
```

`onyx-admin.com`은 Onyx Web frontend입니다. Web `/mcp`가 요청을 내부 backend
`/mcp`로 전달합니다. Backend port와 주소는 공개하지 않습니다. 첫 버전은 인증
header 또는 credential을 요구하지 않습니다.

따라서 `https://onyx-admin.com/mcp`에 접근할 수 있는 client는 모든 검색 대상
문서를 조회할 수 있습니다. 인증이 추가되기 전까지 reverse proxy 또는 배포
네트워크가 접근 범위를 제한해야 합니다. MCP 구현은 client가 보낸 사용자
정보를 신뢰하지 않습니다.

서버는 `search` 도구 하나만 등록합니다.

```json
{
  "query": "검색어",
  "document_sets": ["Engineering", "Operations"],
  "limit": 10
}
```

`limit` 기본값은 10이며 최대값은 20입니다. 결과는 text content와
`structuredContent`에 같은 검색 결과를 제공합니다. `document_sets`를 생략하거나
빈 배열로 보내면 모든 Document Set을 검색합니다. 여러 이름을 보내면 해당
Set들의 합집합만 검색한 뒤 하나의 후보 목록으로 리랭킹합니다.

## Agentic retrieval 유도

도구 설명과 server instructions에 다음 지침을 넣습니다.

- 기본적으로 검색을 한 번만 수행합니다.
- 첫 결과가 부족하거나 질문이 모호할 때만 query를 바꿔 추가 검색합니다.
- 한 요청을 처리할 때 총 검색 호출을 세 번 이하로 제한합니다.
- 같은 의미의 query를 반복하지 않습니다.
- 충분한 근거를 찾으면 추가 검색을 중단합니다.
- 필요한 최소 limit을 사용합니다.
- 결과의 링크와 본문을 근거로 답합니다.

MCP client와 model이 도구 호출을 제어하므로 이 동작은 보장하지 않습니다.
반복 검색을 반드시 보장해야 하면 이후 `SearchService` 내부에 deterministic
multi-query 정책을 추가합니다. MCP sampling은 사용하지 않습니다.
호출 횟수 제한은 server instruction이므로 client 동작을 강제하지 않습니다.
인증과 client 식별이 추가되기 전에는 server-side 호출 횟수 상태를 저장하지
않습니다.

## 오류 처리

- 잘못된 query와 limit은 MCP tool execution error로 반환합니다.
- 존재하지 않는 Document Set 이름은 이름을 포함한 MCP tool execution error로
  반환합니다.
- embedding 또는 OpenSearch 실패는 검색 실패로 반환합니다.
- reranker 실패만 기존 `RerankingService` 정책에 따라 후보 순서로 fallback합니다.
- protocol 오류와 검색 실행 오류를 구분합니다.
- 오류 응답에 credential, 내부 header 또는 전체 backend URL을 넣지 않습니다.

## 파일 경계

- `service/SearchService.kt`: 검색 순서와 결과 병합
- `service/SearchModels.kt`: 검색 입력, 후보, 결과
- `config/McpConfiguration.kt`: MCP transport와 `search` tool 등록
- `ingestion/IngestionWorker.kt`: query embedding, vector mapping 검사, OpenSearch
  검색 지원
- `config/OnyxProperties.kt`: embedding dimension과 검색 제한 설정
- `domain/Repositories.kt`: 요청한 Document Set 이름 확인
- `api/ApiModels.kt`: 기존 rerank API와 service model 변환
- `web/next.config.js`: `/mcp` rewrite
- `SOURCE_PROVENANCE.md`: 새 Kotlin 검색 코드의 FOSS 참고 경로 기록

새 wrapper interface, factory 또는 MCP 전용 검색 구현은 만들지 않습니다.

## 테스트

### 단위 테스트

- query embedding 요청이 `text_type=query`를 전송하는지 확인합니다.
- keyword와 vector 후보가 중복 제거되고 reciprocal-rank 순서로 합쳐지는지
  확인합니다.
- 여러 Document Set 이름이 합집합 `terms` 필터로 두 검색에 적용되는지
  확인합니다.
- 존재하지 않는 Document Set 이름을 거부하는지 확인합니다.
- reranker 성공과 fallback 결과의 limit을 확인합니다.
- 잘못된 query와 limit을 거부하는지 확인합니다.

### OpenSearch 통합 테스트

- 새 index의 `embedding` mapping이 768차원 `knn_vector`인지 확인합니다.
- keyword 검색과 vector 검색이 각 후보를 반환하는지 확인합니다.
- 여러 Document Set에 속한 chunk가 합집합으로 반환되고 다른 Set의 chunk는
  제외되는지 확인합니다.
- 기존 동적 embedding mapping에서는 명확한 reset 오류가 발생하는지 확인합니다.

### MCP 통합 테스트

- `tools/list`가 `search` 하나를 반환하는지 확인합니다.
- `tools/call`이 Document Set 필터를 적용한 구조화 검색 결과를 반환하는지
  확인합니다.
- 잘못된 입력이 tool execution error가 되는지 확인합니다.
- Web `/mcp`가 MCP headers, request body, response stream을 보존하는지 확인합니다.

## 완료 조건

- 원격 MCP client가 `https://onyx-admin.com/mcp` 주소만으로 연결해 `search`를
  발견하고 호출할 수 있습니다.
- 호출 한 번이 query embedding, keyword/vector retrieval, reranking을 수행합니다.
- 여러 Document Set을 지정하면 합집합 후보만 리랭킹합니다.
- REST 또는 MCP transport 세부 정보 없이 `SearchService`를 직접 호출할 수 있습니다.
- reranker가 중단돼도 retrieval 결과를 반환합니다.
- 관련 단위 테스트와 OpenSearch 및 MCP 통합 테스트가 통과합니다.
