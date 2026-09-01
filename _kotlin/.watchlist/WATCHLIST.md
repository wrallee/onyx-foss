# WATCHLIST.md

schema_version: 1
automation: none
timezone: Asia/Seoul
archive_policy: manual

This file records deferred checks. It does not schedule work.

## Open

### WL-20260831-001 — SAML·LDAP 사용자 권한 연동
- status: open
- priority: P2
- owner: both
- due_at: unscheduled
- created_at: 2026-08-31T23:39:08+09:00
- source: Kotlin backend parity design conversation
- trigger: Full permission sync is in scope, but user identity and group mapping are deferred.
- action: MIT FOSS 코드와 공개 표준만 사용해 SAML·LDAP 사용자 및 그룹 연동을 독립 구현한다.
- done_when: EE 코드 없이 사용자 그룹과 문서 ACL 동기화가 검증되고 출처 기록이 남는다.
- last_checked_at:
- result:
- next_step_on_fail: EE 코드 참조를 중단하고 FOSS 전용 clean-room 설계를 다시 검토한다.

### WL-20260901-001 — 첨부파일과 이미지 벡터화 확인
- status: open
- priority: P1
- owner: both
- due_at: unscheduled
- created_at: 2026-09-01T00:41:13+09:00
- source: Kotlin backend FOSS parity implementation review
- trigger: 텍스트 수집만 복원하면 첨부파일 처리와 이미지 벡터화가 누락될 수 있다.
- action: File·Confluence 수집과 indexing 구현에서 첨부파일 추출, 이미지 처리, 벡터화를 다시 확인한다.
- done_when: 첨부파일과 이미지 벡터화의 Python 대응 시나리오가 parity matrix와 Kotlin 테스트에 반영된다.
- last_checked_at:
- result:
- next_step_on_fail: 누락 시 해당 Python 시나리오를 실패 테스트로 추가하고 구현 계획에 반영한다.

### WL-20260901-002 — OpenSearch 인증서 검증 환경변수 복구
- status: open
- priority: P1
- owner: both
- due_at: unscheduled
- created_at: 2026-09-01T20:20:10+09:00
- source: backend/src/main/resources/application.yml OpenSearch configuration
- trigger: Kotlin 백엔드의 OpenSearch 인증서 검증을 임시로 false에 고정했다.
- action: Kotlin 백엔드에서 OPENSEARCH_VERIFY_CERTS 환경변수로 인증서 검증을 제어한다.
- done_when: true와 false 설정이 각각 적용되고 TLS 연결 테스트가 통과한다.
- last_checked_at:
- result:
- next_step_on_fail: Kotlin OpenSearch 클라이언트의 SSL 설정 경로와 환경변수 바인딩을 다시 확인한다.

### WL-20260901-003 — OpenSearch 연결 실패와 재시도 제어
- status: open
- priority: P1
- owner: both
- due_at: unscheduled
- created_at: 2026-09-01T21:17:11+09:00
- source: OpenSearch 장애 동작 확인 대화
- trigger: OpenSearch에 연결할 수 없어도 애플리케이션이 시작되고 connector가 약 1초 간격으로 연결을 반복한다.
- action: 시작 시 OpenSearch 연결을 확인해 실패하면 애플리케이션을 종료하고, 이후 연결 재시도에는 제한된 지수 백오프를 적용한다.
- done_when: OpenSearch 연결 실패 시 애플리케이션이 준비 상태가 되지 않고, 재시도 간격과 최대 빈도가 테스트로 검증된다.
- last_checked_at:
- result:
- next_step_on_fail: 시작 의존성 검사 위치와 connector 작업의 재시도 정책을 분리해 다시 확인한다.

### WL-20260901-004 — OpenSearch 장애 중 connector 삭제 복구
- status: open
- priority: P1
- owner: both
- due_at: unscheduled
- created_at: 2026-09-01T21:17:11+09:00
- source: backend/src/main/kotlin/com/onyx/foss/kotlin/service/AdminService.kt; backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/IngestionWorker.kt; web/src/app/admin/indexing/status/CCPairIndexingStatusTable.tsx
- trigger: OpenSearch 인증 또는 접속 장애 중 삭제하면 DB는 먼저 DELETING이 되지만 외부 삭제 실패 후 복구 경로가 없다. RUNNING job과 IN_PROGRESS attempt도 종료되지 않는다.
- action: 삭제 요청을 재시도 가능하게 만들고, 활성 job과 attempt를 취소 또는 실패 상태로 종료한다. 목록은 DELETING pair를 INITIAL_INDEXING으로 덮어쓰지 않게 수정한다.
- done_when: 인증 정상과 OpenSearch 401·접속 실패 조건의 삭제 테스트가 통과한다. 상세·목록·attempt 화면은 동일한 상태 전이를 표시하고, job·attempt·pair·connector·OpenSearch 데이터가 일관된다.
- last_checked_at: 2026-09-01T21:46:37+09:00
- result: OpenShift에서 삭제 요청이 OpenSearch 401 Unauthorized로 실패했다. Backend은 인증 환경변수를 받지만 WebClient에 Authorization 헤더를 설정하지 않았다. pair는 DELETING, job은 RUNNING, attempt는 IN_PROGRESS로 남았다. 목록 UI는 완료 attempt가 없으면 pair 상태를 무시하고 INITIAL_INDEXING으로 표시했다. 1883fa04b에서 배포의 OPENSEARCH_ADMIN_USERNAME·OPENSEARCH_ADMIN_PASSWORD를 Basic Auth로 연결했다.
- next_step_on_fail: 인증 수정과 삭제 상태 전이를 분리해 테스트한다. 그런 다음 장애 후 재시도 또는 롤백 규칙을 결정한다.

### WL-20260901-005 — Confluence space probe의 lazy 계약 복구
- status: open
- priority: P1
- owner: both
- due_at: unscheduled
- created_at: 2026-09-01T21:42:27+09:00
- source: backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/ConfluenceConnectorLoader.kt
- trigger: Kotlin의 eager space 목록 조회를 피하려고 credential 검증에서 첫 페이지만 직접 읽는 임시 수정을 적용했다.
- action: Confluence space 조회를 Python generator와 같은 lazy 계약으로 정리하고 probe와 전체 pagination의 책임을 분리한다.
- done_when: probe는 첫 space에서 중단되고 전체 space 조회는 모든 페이지를 처리하며 Cloud v2 fallback까지 테스트로 검증된다.
- last_checked_at:
- result:
- next_step_on_fail: probe 전용 API와 전체 space pagination API를 분리하는 설계를 다시 검토한다.

### WL-20260901-006 — OpenSearch 공식 Java client 전환
- status: open
- priority: P1
- owner: both
- due_at: unscheduled
- created_at: 2026-09-01T21:54:32+09:00
- source: backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/IngestionWorker.kt
- trigger: Kotlin backend이 WebClient로 OpenSearch REST API와 JSON, 인증, TLS, 오류 처리를 직접 관리한다.
- action: OpenSearchIndexer를 opensearch-java와 ApacheHttpClient5Transport 기반으로 전환한다.
- done_when: 인덱스 생성, mapping, upsert, update-by-query, delete-by-query, reindex, alias 교체가 공식 client로 작동하고 인증·TLS·timeout 설정이 통합 테스트로 검증된다.
- last_checked_at:
- result:
- next_step_on_fail: 공식 client에 typed API가 없는 작업만 generic transport로 분리한다.

## Done

## Archive
