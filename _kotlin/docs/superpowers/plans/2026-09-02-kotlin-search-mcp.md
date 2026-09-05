# Kotlin 검색 MCP 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 원격 MCP client가 `https://onyx-admin.com/mcp`에서 Document Set 합집합 필터를 지원하는 검색 도구를 사용하게 한다.

**Architecture:** Web은 공개 `/mcp` 요청을 내부 Kotlin backend `/mcp`로 전달한다. MCP transport는 `SearchService`를 직접 호출한다. `SearchService`는 query embedding, keyword/vector 후보 검색, reciprocal-rank 병합, reranking을 순서대로 수행한다.

**Tech Stack:** Kotlin 2.1.20, Java 21, Spring Boot 3.4.5, MCP Java SDK 2.0.1, Jackson 2, OpenSearch 3.6.0, JUnit 5, MockWebServer, Testcontainers

**Spec:** `docs/superpowers/specs/2026-09-02-kotlin-search-mcp-design.md`

## Global Constraints

- Spring Boot와 Spring AI는 이번 작업에서 변경하지 않는다.
- MCP client는 Web의 `https://onyx-admin.com/mcp`만 사용한다.
- Backend 주소와 port를 외부에 노출하지 않는다.
- 첫 버전은 인증과 ACL 필터를 구현하지 않는다.
- 여러 Document Set은 합집합으로 적용한다.
- 일반 검색은 기본 30개 후보만 rerank한다.
- Agentic retrieval 지침은 기본 1회, 최대 3회 검색을 권장한다.
- 새 production 동작은 실패 테스트를 먼저 작성한다.
- Enterprise 코드와 `_kotlinmania`는 참조하지 않는다.

---

## Issues to Address

- Kotlin backend에 query embedding 호출이 없다.
- OpenSearch index의 embedding은 vector search mapping이 아니다.
- Kotlin backend에 keyword 또는 vector retrieval이 없다.
- 여러 Document Set 이름을 검색 필터로 받을 수 없다.
- 검색과 reranking을 묶는 공통 service가 없다.
- MCP endpoint와 search tool이 없다.
- Web에 공개 `/mcp` 진입점이 없다.
- Agentic retrieval 비용을 제한하는 MCP 지침이 없다.

## Important Notes

- Python 검색은 Document Set 이름 목록을 OpenSearch `terms` query로 처리한다.
- `terms` query는 목록 중 하나와 일치하면 포함하므로 합집합이다.
- Kotlin의 `RERANKER_MAX_DOCUMENTS=100`은 안전 상한이다.
- 새 검색의 기본 rerank 후보 수는 30이다.
- OpenSearch의 기존 숫자 배열 embedding mapping은 `knn_vector`로 직접 변경할 수 없다.
- 구축 단계이므로 기존 embedding은 migration하지 않는다.
- 사용자가 정확한 Onyx index를 삭제하고 connector를 다시 실행한다.
- 애플리케이션은 index를 자동 삭제하지 않는다.
- MCP Java SDK core Servlet transport를 사용한다. Spring AI transport는 사용하지 않는다.
- 인증 전에는 `/mcp` 접근자가 전체 검색 대상 문서를 조회할 수 있다.
- Web proxy는 MCP headers, request body, response stream을 보존해야 한다.

## Implementation strategy

1. MCP SDK와 검색 설정을 추가한다.
2. `ModelServerClient`에 query embedding 경로를 추가한다.
3. 새 OpenSearch index를 768차원 `knn_vector` mapping으로 생성한다.
4. keyword와 vector 후보 검색에 같은 Document Set `terms` 필터를 적용한다.
5. `SearchService`에서 후보를 reciprocal rank로 병합하고 기존 reranker를 호출한다.
6. MCP `search` tool과 제한적인 agentic retrieval 지침을 등록한다.
7. Web `/mcp`를 내부 `/api/mcp` proxy로 연결한다.
8. source provenance와 운영 문서를 갱신한다.

## Tasks

### Task 1: 검색 설정과 query embedding

- [x] Query embedding payload를 검증하는 실패 테스트를 작성한다.
- [x] 실패 이유가 `text_type=query` 지원 부재인지 확인한다.
- [x] 검색 후보 50, rerank 후보 30, embedding dimension 768 설정을 추가한다.
- [x] `ModelServerClient`가 passage와 query embedding을 공통 호출로 처리하게 한다.
- [x] 관련 단위 테스트를 통과시킨다.

### Task 2: OpenSearch vector mapping과 reset 검사

- [x] 새 index의 `knn_vector` mapping을 검증하는 실패 통합 테스트를 작성한다.
- [x] 기존 동적 embedding mapping의 reset 안내 실패 테스트를 작성한다.
- [x] `index.knn`, dimension, Lucene HNSW cosine mapping을 추가한다.
- [x] 기존 incompatible embedding mapping을 감지하고 쓰기와 검색을 중단한다.
- [x] mapping과 reset 검사 통합 테스트를 통과시킨다.

### Task 3: Document Set 합집합 retrieval

- [x] keyword와 vector 검색의 Document Set 합집합 동작을 검증하는 실패 테스트를 작성한다.
- [x] 존재하지 않는 Document Set 이름의 실패 동작을 검증한다.
- [x] `DocumentSetRepository`에 이름 일괄 조회를 추가한다.
- [x] OpenSearch keyword와 vector 후보 검색을 추가한다.
- [x] 두 검색에 같은 `terms` 필터를 적용한다.
- [x] 단위 및 OpenSearch 통합 테스트를 통과시킨다.

### Task 4: 공통 SearchService

- [x] 후보 중복 제거와 reciprocal-rank 병합의 실패 테스트를 작성한다.
- [x] reranker 성공과 fallback limit의 실패 테스트를 작성한다.
- [x] 검색 input, candidate, result model을 추가한다.
- [x] query embedding, retrieval, fusion, reranking 순서를 구현한다.
- [x] 기본 30개 후보와 설정 상한을 적용한다.
- [x] SearchService 단위 테스트를 통과시킨다.

### Task 5: MCP server와 search tool

- [x] `tools/list`와 `tools/call`의 실패 통합 테스트를 작성한다.
- [x] 잘못된 query, limit, Document Set 입력의 tool error를 검증한다.
- [x] MCP Java SDK core Servlet transport를 `/mcp`에 등록한다.
- [x] `search` tool schema와 structured result를 등록한다.
- [x] 기본 1회와 최대 3회를 권장하는 server instructions를 추가한다.
- [x] MCP 통합 테스트를 통과시킨다.

### Task 6: Web proxy와 문서

- [x] Web `/mcp`의 request와 streaming response 보존 테스트를 작성한다.
- [x] Web `/mcp`를 기존 `/api/mcp` catch-all proxy로 연결한다.
- [x] 원격 URL과 무인증 보안 경계를 README에 기록한다.
- [x] 새 검색 코드의 FOSS 참고 경로를 `SOURCE_PROVENANCE.md`에 기록한다.
- [x] Web type check와 proxy 테스트를 통과시킨다.

### Task 7: 전체 검증

- [x] Backend 단위 테스트를 실행한다.
- [x] OpenSearch 통합 테스트를 실행한다.
- [x] 사용자의 index 삭제와 connector 재실행이 필요한 상태를 보고한다.
- [x] MCP 요청을 Web 경유로 live 검증한다.
- [x] 변경 파일에 대한 formatting과 정적 검사를 실행한다.
- [x] 실패와 경고가 없는지 확인한다.

## Tests

- **Unit:** query embedding payload, 입력 검증, reciprocal-rank fusion, reranker fallback
- **External dependency unit:** MockWebServer 기반 model-server와 OpenSearch request 계약
- **Integration:** OpenSearch `knn_vector` mapping, reset 검사, keyword/vector retrieval, Document Set 합집합
- **MCP integration:** tool discovery, tool call, structured result, tool execution error
- **Web:** `/mcp` header, body, stream proxy 보존
- **Live:** `https://onyx-admin.com/mcp`와 같은 Web 경로를 통한 MCP discovery와 search 호출
- **Playwright:** UI 변경이 없으므로 추가하지 않는다.
