# Kotlin Backend FOSS 동작 호환 설계

## 배경

현재 Kotlin backend는 Onyx FOSS API 형태를 많이 복제했습니다.
그러나 실제 동작은 상당 부분 빠져 있습니다.

현재 테스트는 주로 응답 형태와 외부 API의 정상 응답만 확인합니다.
DB 동작, 수집 복구, pruning, 커넥터 호환성은 충분히 검증하지 않습니다.

상위 Python checkout에는 필요한 동작과 테스트가 이미 있습니다.
이 설계는 해당 구현을 동작 기준으로 사용합니다.

## 목표

Kotlin backend에 이미 존재하는 기능군의 FOSS 동작을 복원합니다.
Python 동작과의 차이를 감지하는 테스트를 추가합니다.

API 모양만 같은 것은 완료로 보지 않습니다.
DB 상태, checkpoint, 오류, 색인 문서, 권한 metadata도 같아야 합니다.

## 범위

### 포함

- Credential, Connector, CC Pair, Document Set, 파일 관리
- File, Jira, Confluence, GitHub 커넥터
- 커넥터 검증, pagination, checkpoint, 부분 실패, rate limit
- 수집 job, attempt, 상태 전이, 재시도, 복구
- Embedding 요청과 OpenSearch 쓰기 및 삭제
- Pruning과 문서 조회 실패의 안전한 처리
- 전체 문서 권한 동기화
- 권한 동기화 attempt와 API 계약
- 위 기능에 사용하는 기존 Web 계약

### 제외

- Kotlin source와 활성 진입점이 없는 커넥터
- 사용자 가입, 인증, session, 사용자별 설정
- Enterprise 기능과 모든 Enterprise source code
- Multitenancy
- External group sync
- SAML, LDAP, OIDC, SCIM 연동

## Source 경계

다음 순서로 동작 기준을 정합니다.

1. 상위 Python FOSS 구현의 실제 동작
2. 상위 Python FOSS 테스트의 기대 결과
3. 기존 Web 요청 및 응답 계약
4. 현재 Kotlin 문서

Kotlin README가 미완성 port를 설명하면 Python 구현을 우선합니다.
인증, Enterprise, multitenancy, external group sync는 계속 제외합니다.

`ee/` 디렉터리와 Enterprise license source는 사용하지 않습니다.
Port한 동작과 복사한 fixture의 출처를 기록합니다.

## 기능 경계

커넥터 종류를 늘리지 않고 기존 커넥터의 완성도를 높입니다.

구현된 커넥터에서는 Python 구현의 누락 동작을 복원합니다.
Python 테스트가 있다는 이유로 다른 커넥터를 추가하지 않습니다.

포함된 커넥터나 관리 흐름에 필요한 공통 동작만 추가합니다.
추후 사용을 위한 호환 framework는 만들지 않습니다.

## 구조

### 관리 수명주기

관리 계층은 다음 동작을 담당합니다.

- Credential 생성, masking, 수정, 연결 검사, 삭제
- Connector 생성, 수정, 연결, 일시정지, 실행, 삭제
- CC Pair metadata, 상태, 색인 요약, attempt 기록
- Document Set 구성원, 수정, 삭제, 공개 상태
- 파일 upload, 교체, 제거, metadata, connector 수정

기존 Spring controller와 service 경계가 적합하면 그대로 사용합니다.
승인된 동작을 현재 구조에 넣을 수 없을 때만 변경합니다.

### 동기식 Connector Batch

Python 커넥터는 동기식 generator를 사용합니다.
Kotlin에서는 동기식 `Sequence`를 사용합니다.

각 connector batch는 다음 값을 전달합니다.

- 문서
- Connector failure
- 다음 connector checkpoint
- 남은 작업 여부

File 수집은 batch 하나를 반환할 수 있습니다.
원격 커넥터는 API page 또는 checkpoint 단위로 반환합니다.

Coroutine `Flow`나 비동기 connector framework는 추가하지 않습니다.

### 수집 흐름

수집 worker는 connector batch를 한 번에 하나씩 처리합니다.

1. PostgreSQL lock으로 실행 가능한 job 하나를 가져옵니다.
2. 해당 attempt를 실행 중으로 변경합니다.
3. Connector batch 하나를 불러옵니다.
4. 문서를 변환하고 chunk로 나눈 뒤 embedding하고 색인합니다.
5. 문서 실패를 식별자와 문맥 정보와 함께 저장합니다.
6. Python과 같은 안전한 시점에 checkpoint를 저장합니다.
7. Connector가 완료를 알릴 때까지 계속합니다.
8. 전체 조회가 끝난 뒤 Python과 같은 pruning을 적용합니다.
9. Attempt와 CC Pair의 최종 상태를 설정합니다.

Worker는 원격 문서 전체를 메모리에 적재하지 않습니다.

### 오류 처리

모든 커넥터에 적용하는 단일 재시도 정책을 만들지 않습니다.
각 커넥터의 Python 동작을 개별적으로 port합니다.

- 문서 단위 `ConnectorFailure`는 부분 실패로 처리합니다.
- 부분 실패 결과는 `COMPLETED_WITH_ERRORS`로 끝날 수 있습니다.
- 처리하지 못한 치명적 오류는 attempt를 `FAILED`로 끝냅니다.
- 성공한 문서에 해당하는 이전 오류만 해결 상태로 변경합니다.
- 반복 오류 상태는 Python의 연속 실패 규칙을 따릅니다.
- 조회에 실패한 문서 ID는 pruning 중에도 보존합니다.
- Checkpoint는 Python과 같은 안전한 시점에만 전진합니다.
- Python이 문서 실패로 처리하는 embedding과 index 오류도 동일하게 처리합니다.

Rate limit 처리도 커넥터별로 유지합니다.
GitHub, Confluence, Jira는 각 Python 정책을 따라야 합니다.

API 응답, log, 예외에 credential 값을 노출하지 않습니다.

### 문서 권한 동기화

문서 권한 동기화는 FOSS 커넥터 동작이므로 포함합니다.

지원하는 외부 문서 접근 metadata를 수집하고 저장합니다.
권한 동기화 attempt와 실패 상태도 추적합니다.
일시적인 권한 조회 실패 때문에 문서를 pruning하지 않습니다.

현재 접근 규칙을 사용하는 내부 사용자는 없습니다.
검색 시점의 사용자 권한 적용은 identity 기능을 추가할 때 구현합니다.

External group sync는 제외합니다.
Onyx의 해당 실행 구현은 Enterprise 영역입니다.

## 테스트 전략

### 빠른 테스트

검증, 변환, 상태 계산에는 JUnit을 사용합니다.
새 utility를 추가하기 전에 기존 helper와 dependency를 사용합니다.

### Connector 계약 테스트

원격 connector 테스트에는 MockWebServer를 사용합니다.
네 커넥터에 적용되는 Python 시나리오를 모두 port합니다.

Pagination, 인증 header, 검증, checkpoint, 실패, rate limit을 검사합니다.
커넥터별 문서 권한 조회도 검사합니다.

일반 테스트에는 실제 Jira, Confluence, GitHub 계정이 필요하지 않습니다.
실제 credential을 사용하는 smoke test는 선택 테스트로 분리합니다.

### PostgreSQL 통합 테스트

DB 통합 테스트에는 격리된 PostgreSQL container를 사용합니다.
실제 Flyway migration을 실행합니다.

DB 통합 테스트에 H2를 사용하지 않습니다.
현재 schema는 PostgreSQL JSONB, cast, timestamp, identity 동작을 사용합니다.
Job 선점에는 `FOR UPDATE SKIP LOCKED` 동작도 필요합니다.

CRUD, foreign key, unique 제약, transaction, pagination, 동시 선점을 검증합니다.

### 전체 수집 테스트

대표 File 전체 수집 흐름에는 Docker Compose를 사용합니다.
이 흐름은 PostgreSQL, model server, OpenSearch를 포함합니다.

Backend 요청은 Web service를 통해 전송합니다.
최종 API 상태, DB row, OpenSearch 문서를 확인합니다.

작은 테스트로 증명할 수 없는 동작에만 전체 수집 테스트를 추가합니다.

## 시나리오 목록

| 영역 | 필수 시나리오 그룹 |
| --- | --- |
| 관리 | 생성, 수정, 연결, 잘못된 연결 거부, 일시정지, 실행, 삭제 |
| File | 다중 파일, metadata, 교체, 제거, parsing, checkpoint 완료, pruning |
| Jira | Project 범위, JQL, pagination, checkpoint, 건너뛴 issue, typed error, rate limit, 권한 |
| Confluence | Cloud와 Server, page, attachment, comment, HTML, checkpoint, rate limit, 권한 |
| GitHub | Public·private repository, branch, issue, pull request, file, checkpoint, rate limit, 권한 |
| 수집 | 선점, 상태 전이, 부분 실패, 전체 실패, 복구, checkpoint, 반복 오류 |
| Pruning | 누락 문서, 조회 실패, index 삭제 실패, DB 일관성 |
| 권한 동기화 | Attempt 상태, 부분 실패, 저장된 ACL, 안전한 재시도 |
| API 계약 | 응답 field, 상태 값, pagination, 검증 오류, Web 호환성 |

구현 전에 각 Kotlin 시나리오를 Python source test 또는 구현 경로와 연결합니다.
명시적으로 제외한 기능에 의존하는 시나리오만 제거합니다.

## 완료 기준

- 적용 가능한 각 Python FOSS 시나리오에 Kotlin 테스트 또는 동등한 검증이 있습니다.
- 테스트가 구현 변경 전에 누락 동작을 재현합니다.
- 빠른 테스트와 connector 계약 테스트가 모두 통과합니다.
- PostgreSQL 통합 테스트가 실제 PostgreSQL에서 통과합니다.
- 대표 Docker File 수집 테스트가 Web service를 통해 통과합니다.
- Checkpoint, 부분 실패, 복구, pruning이 Python 동작과 일치합니다.
- 지원 커넥터가 문서 ACL을 수집하고 attempt 상태와 함께 저장합니다.
- 새 connector 종류를 추가하지 않습니다.
- Enterprise code와 fixture를 복사하거나 번역하지 않습니다.
- 기존 Web 관리 흐름이 계속 동작합니다.

## 후속 작업

사용자 identity와 검색 시점 ACL 적용은 후속 작업으로 남깁니다.
향후 SAML 또는 LDAP는 MIT FOSS code와 공개 표준만 사용해야 합니다.
Onyx Enterprise source code에서 파생하면 안 됩니다.
