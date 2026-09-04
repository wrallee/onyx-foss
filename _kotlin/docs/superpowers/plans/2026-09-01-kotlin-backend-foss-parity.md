# Kotlin Backend FOSS 동작 호환 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 기존 Kotlin 관리 기능과 File·Jira·Confluence·GitHub 커넥터를 Python FOSS 동작 수준으로 복원하고 회귀 테스트를 구축한다.

**Architecture:** Python의 동기 generator를 Kotlin `Sequence<ConnectorBatch>`로 대응시킨다. 커넥터별 로더는 분리하지만 공통 retry framework는 만들지 않는다. 실제 PostgreSQL, MockWebServer, Docker Compose를 계층별로 사용한다.

**Tech Stack:** Kotlin 2.1.20, Java 21, Spring Boot 3.4.5, JUnit 5, Mockito, MockWebServer, Testcontainers PostgreSQL, Flyway, Apache Tika, PostgreSQL 15.2, OpenSearch 3.6.0

**Spec:** `docs/superpowers/specs/2026-09-01-kotlin-backend-foss-parity-design.md`

## Global Constraints

- Python FOSS 구현과 테스트를 동작 기준으로 사용한다.
- `ee/` 코드와 Enterprise fixture를 열람, 복사, 번역하지 않는다.
- File, Jira, Confluence, GitHub 외 커넥터를 추가하지 않는다.
- 사용자, 인증, multitenancy, external group sync를 추가하지 않는다.
- 모든 커넥터 호출은 동기식으로 유지한다. Coroutine `Flow`를 추가하지 않는다.
- DB 통합 테스트는 H2가 아닌 PostgreSQL 15.2에서 실행한다.
- 실제 Jira, Confluence, GitHub 계정은 필수 테스트에서 사용하지 않는다.
- Backend live 요청은 `http://localhost:3000`의 Web service를 통한다.
- 변경한 모든 Kotlin과 TypeScript 코드는 엄격한 type을 사용한다.
- 각 작업은 실패 테스트, 최소 구현, 통과 확인, 커밋 순서를 지킨다.

---

## Issues to Address

- 관리 API의 DB 제약, 삭제, pagination, 오류 계약 테스트가 없다.
- 원격 커넥터가 모든 문서를 `List`에 적재한다.
- Checkpoint가 커넥터 cursor가 아닌 마지막 성공 시각만 저장한다.
- 문서 단위 실패와 치명적 실패를 구분하지 않는다.
- 성공 한 번이 모든 과거 오류를 resolved 처리한다.
- 단일 실패가 즉시 repeated-error 상태를 만든다.
- Pruning과 문서별 OpenSearch 삭제가 없다.
- File 교체 시 `file_locations`와 `file_names`가 어긋날 수 있다.
- Jira, Confluence, GitHub의 검증, pagination, retry, permission 동작이 불완전하다.
- Permission sync API가 항상 `applicable=false`를 반환한다.
- 현재 테스트는 실제 PostgreSQL migration과 lock 동작을 검증하지 않는다.

## Important Notes

- 관련 Python 테스트는 현재 186개다. 각 시나리오를 포함, 제외, 대체 검증으로 분류한다.
- 제외는 승인된 범위 밖 기능에만 허용한다.
- Python의 repeated-error 기준은 refresh connector에서 연속 실패 5회다.
- Refresh frequency가 없는 connector에서는 실패 1회가 repeated-error 상태를 만든다.
- `ExternalAccess`는 user email set, external group ID set, public flag로 구성한다.
- Permission ACL은 저장하지만 사용자 기반 검색 필터는 구현하지 않는다.
- Retrieval 실패 문서 ID는 pruning 대상에서 제외한다.
- GitHub, Confluence, Jira retry 정책은 서로 다르다.
- 기존 `V1__connector_admin_and_ingestion.sql`은 수정하지 않는다. 새 migration만 추가한다.
- `.watchlist/WATCHLIST.md`는 local 후속 기록이며 구현 입력으로 사용하지 않는다.

## Implementation strategy

1. Python parity matrix와 PostgreSQL test harness를 먼저 만든다.
2. 관리 API의 현재 DB 동작을 통합 테스트로 고정하고 차이를 수정한다.
3. Batch, checkpoint, failure, ACL domain type과 migration을 추가한다.
4. 수집 processor를 batch 단위로 바꾸고 오류 복구와 pruning을 구현한다.
5. File, Jira, Confluence, GitHub 순서로 Python 시나리오를 port한다.
6. 각 커넥터의 permission retrieval을 구현한 뒤 permission worker를 연결한다.
7. 대표 File 수집을 전체 Docker stack에서 검증한다.

## Tests

- **Unit:** domain validation, cursor parsing, retry 계산, content 변환
- **External dependency unit:** MockWebServer 기반 Jira, Confluence, GitHub 계약
- **Integration:** Testcontainers PostgreSQL 기반 migration, CRUD, lock, 상태 전이
- **Playwright:** 새 UI 동작이 없으므로 추가하지 않는다.
- **Live stack:** Web → API → PostgreSQL → model server → OpenSearch File 수집 1건
- **Optional smoke:** 실제 SaaS credential이 있을 때만 커넥터 validation을 실행한다.

## File Structure

### 공통 및 DB

- `backend/build.gradle.kts`: Testcontainers dependency와 integration test 설정
- `backend/gradlew`, `backend/gradlew.bat`, `backend/gradle/wrapper/*`: 고정 Gradle 실행기
- `backend/PYTHON_PARITY.md`: Python 시나리오와 Kotlin 검증의 대응표
- `backend/src/test/kotlin/com/onyx/foss/kotlin/support/PostgresIntegrationTest.kt`: PostgreSQL test base
- `backend/src/main/resources/db/migration/V2__ingestion_parity_and_permissions.sql`: failure, ACL, permission attempt schema
- `backend/src/main/kotlin/com/onyx/foss/kotlin/domain/Domain.kt`: 신규 entity와 enum
- `backend/src/main/kotlin/com/onyx/foss/kotlin/domain/Repositories.kt`: 신규 조회와 lock query

### 수집 core

- `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/ConnectorModels.kt`: document, batch, checkpoint, failure, ACL
- `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/IngestionWorker.kt`: batch orchestration과 상태 전이
- `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/IngestionScheduler.kt`: refresh와 full-prune job 생성
- `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/PruningService.kt`: 누락 문서 계산과 삭제
- `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/RemoteJsonClient.kt`: HTTP 요청과 connector별 retry 지원점
- `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/RemoteConnectorLoaders.kt`: source dispatch만 담당
- `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/FileConnectorLoader.kt`: File 변환
- `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/JiraConnectorLoader.kt`: Jira 수집
- `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/ConfluenceConnectorLoader.kt`: Confluence 수집
- `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/GithubConnectorLoader.kt`: GitHub 수집

### Permission sync

- `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/PermissionSyncWorker.kt`: attempt 생성, lock, ACL upsert
- `backend/src/main/kotlin/com/onyx/foss/kotlin/api/AdminController.kt`: 실제 permission attempt 응답
- `backend/src/main/kotlin/com/onyx/foss/kotlin/service/IngestionQueryService.kt`: permission attempt pagination

### Test files

- `backend/src/test/kotlin/com/onyx/foss/kotlin/api/AdminApiIntegrationTest.kt`
- `backend/src/test/kotlin/com/onyx/foss/kotlin/domain/SchemaV2IntegrationTest.kt`
- `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/ConnectorModelsTest.kt`
- `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/IngestionProcessorIntegrationTest.kt`
- `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/PruningServiceTest.kt`
- `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/FileConnectorLoaderTest.kt`
- `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/JiraConnectorLoaderTest.kt`
- `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/ConfluenceConnectorLoaderTest.kt`
- `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/GithubConnectorLoaderTest.kt`
- `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/PermissionSyncIntegrationTest.kt`
- `scripts/test-kotlin-file-ingestion.sh`: live stack 검증

---

### Task 1: Python parity matrix와 PostgreSQL test harness

**Files:**
- Create: `backend/PYTHON_PARITY.md`
- Create: `backend/gradlew`
- Create: `backend/gradlew.bat`
- Create: `backend/gradle/wrapper/gradle-wrapper.jar`
- Create: `backend/gradle/wrapper/gradle-wrapper.properties`
- Modify: `backend/build.gradle.kts`
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/support/PostgresIntegrationTest.kt`
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/domain/MigrationSmokeTest.kt`

**Interfaces:**
- Consumes: Docker Engine and `postgres:15.2-alpine`
- Produces: `abstract class PostgresIntegrationTest` for all DB integration tests
- Produces: `PYTHON_PARITY.md` rows with `planned`, `covered`, or `excluded` status

- [ ] **Step 1: Generate the Gradle 8.14.3 wrapper**

Run:

```bash
docker run --rm -v "$PWD/backend:/workspace" -w /workspace gradle:8.14.3-jdk21 \
  gradle wrapper --gradle-version 8.14.3 --distribution-type bin
```

Expected: `backend/gradlew` and `backend/gradle/wrapper/gradle-wrapper.properties` exist.

- [ ] **Step 2: Add PostgreSQL Testcontainers dependencies**

Add to `dependencies` in `backend/build.gradle.kts`:

```kotlin
testImplementation("org.testcontainers:junit-jupiter")
testImplementation("org.testcontainers:postgresql")
```

Spring Boot dependency management supplies the compatible version.

- [ ] **Step 3: Write the failing migration smoke test**

Create `MigrationSmokeTest.kt`:

```kotlin
class MigrationSmokeTest : PostgresIntegrationTest() {
    @Autowired lateinit var jdbc: JdbcTemplate

    @Test
    fun `flyway creates the current schema`() {
        val tables = jdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            String::class.java,
        )
        assertTrue("ingestion_jobs" in tables)
        assertTrue("indexed_documents" in tables)
    }
}
```

- [ ] **Step 4: Run the smoke test and verify failure**

Run:

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test \
  --tests com.onyx.foss.kotlin.domain.MigrationSmokeTest
```

Expected: compilation fails because `PostgresIntegrationTest` does not exist.

- [ ] **Step 5: Implement the PostgreSQL integration base**

Create `PostgresIntegrationTest.kt`:

```kotlin
@Testcontainers
@SpringBootTest(
    properties = [
        "onyx.worker.enabled=false",
        "onyx.storage.root=/tmp/onyx-kotlin-tests",
        "onyx.crypto.key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
    ],
)
abstract class PostgresIntegrationTest {
    companion object {
        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer<Nothing>("postgres:15.2-alpine")

        @DynamicPropertySource
        @JvmStatic
        fun database(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url", postgres::getJdbcUrl)
            registry.add("spring.datasource.username", postgres::getUsername)
            registry.add("spring.datasource.password", postgres::getPassword)
        }
    }
}
```

- [ ] **Step 6: Run the migration smoke test**

Run the Step 4 command.

Expected: PASS and Flyway validates `V1__connector_admin_and_ingestion.sql` against PostgreSQL.

- [ ] **Step 7: Create the Python parity matrix**

Add one row for every test function under these FOSS paths:

```text
backend/tests/integration/tests/{connector,document_set,index_attempt,indexing,pruning}
backend/tests/daily/connectors/{file,jira,confluence,github}
backend/tests/unit/onyx/connectors/{jira,confluence,github}
backend/tests/external_dependency_unit/permission_sync/test_cc_pair_sync_attempts_routes.py
```

Use this exact table shape:

```markdown
| Python source | Scenario | Kotlin test | Status | Exclusion reason |
| --- | --- | --- | --- | --- |
| `.../test_connector_creation.py::test_connector_creation` | connector create and first run | `AdminApiIntegrationTest.connectorCreationQueuesFirstRun` | planned | |
```

Mark only user, Enterprise, multitenancy, external-group-sync, and unrelated connector scenarios as `excluded`.

- [ ] **Step 8: Verify the matrix has no unclassified row**

Run:

```bash
rg '\| (planned|covered|excluded) \|' backend/PYTHON_PARITY.md | wc -l
```

Expected: the count equals the number of scenario rows recorded in the matrix.

- [ ] **Step 9: Commit the harness and matrix**

```bash
git add _kotlin/backend/build.gradle.kts _kotlin/backend/gradlew _kotlin/backend/gradlew.bat \
  _kotlin/backend/gradle _kotlin/backend/PYTHON_PARITY.md _kotlin/backend/src/test
git commit -m "test: add Kotlin backend parity harness"
```

---

### Task 2: 관리 API와 DB 수명주기

**Files:**
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/api/AdminApiIntegrationTest.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/service/AdminService.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/service/FileStorageService.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/service/IngestionQueryService.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/api/ApiExceptionHandler.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/domain/Repositories.kt`
- Modify: `backend/PYTHON_PARITY.md`

**Interfaces:**
- Consumes: `PostgresIntegrationTest`
- Produces: Python-compatible create, associate, pause, run, delete, Document Set, and pagination behavior

- [ ] **Step 1: Write failing credential and connector lifecycle tests**

Add tests with these exact assertions:

```kotlin
@Test fun credentialSecretsAreMaskedAndNeverReturned()
@Test fun credentialUpdatePreservesMaskedSecretFields()
@Test fun associatedCredentialCannotBeDeleted()
@Test fun connectorRejectsCredentialFromAnotherSource()
@Test fun duplicateAssociationReturnsTheExistingPairWithoutASecondJob()
@Test fun overlappingConnectorsRemainIndependent()
@Test fun deletingOneOverlappingPairKeepsTheConnector()
@Test fun deletingTheLastPairDeletesTheConnector()
```

Assert HTTP status, response body, repository counts, and queued job counts in every test.

- [ ] **Step 2: Run the lifecycle tests and verify failures**

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test \
  --tests com.onyx.foss.kotlin.api.AdminApiIntegrationTest
```

Expected: masked credential update fails because the current code stores the mask as the new secret.

- [ ] **Step 3: Preserve masked credential fields during update**

Merge masked request fields with the decrypted current credential before encryption:

```kotlin
private fun mergeMaskedCredential(current: JsonNode, update: JsonNode): JsonNode {
    if (!current.isObject || !update.isObject) return update
    val merged = update.deepCopy<ObjectNode>()
    update.fields().forEach { (name, value) ->
        if (value.isTextual && value.asText() == "********") merged.set<JsonNode>(name, current.path(name))
    }
    return merged
}
```

Call this helper only from credential update. Credential creation must store the submitted value unchanged.

- [ ] **Step 4: Write failing Document Set and pagination tests**

```kotlin
@Test fun documentSetRejectsMissingPair()
@Test fun documentSetNameIsUnique()
@Test fun deletingPairRemovesItsDocumentSetMembership()
@Test fun indexAttemptPaginationUsesZeroBasedPages()
@Test fun paginationRejectsNegativePageAndNonPositivePageSize()
```

Assert the DB join rows and `total_items` values.

- [ ] **Step 5: Run the new tests and verify failures**

Run the Task 2 test command.

Expected: invalid pagination currently does not return a controlled 400 response.

- [ ] **Step 6: Implement pagination validation and stable API errors**

Add one shared private check in `IngestionQueryService`:

```kotlin
private fun validatePage(page: Int, pageSize: Int) {
    require(page >= 0) { "page_num must be non-negative" }
    require(pageSize > 0) { "page_size must be positive" }
}
```

Call it from both `attempts` and `errors`. Keep `ApiExceptionHandler.invalidInput` as the single 400 mapping.

- [ ] **Step 7: Write and fix the File metadata alignment test**

Test this sequence:

1. Upload files `a.txt` and `b.txt`.
2. Remove only `a.txt`.
3. Add `c.txt`.
4. Assert locations and names are `[b, c]` in the same order.

Replace the independent name list mutation with paired ID/name records before filtering.

- [ ] **Step 8: Run management tests**

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test \
  --tests 'com.onyx.foss.kotlin.api.*' \
  --tests 'com.onyx.foss.kotlin.service.*'
```

Expected: PASS.

- [ ] **Step 9: Mark covered management rows and commit**

```bash
git add _kotlin/backend/src/main _kotlin/backend/src/test _kotlin/backend/PYTHON_PARITY.md
git commit -m "fix: align Kotlin management lifecycle"
```

---

### Task 3: Batch, checkpoint, failure, ACL domain

**Files:**
- Create: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/ConnectorModels.kt`
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/ConnectorModelsTest.kt`
- Create: `backend/src/main/resources/db/migration/V2__ingestion_parity_and_permissions.sql`
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/domain/SchemaV2IntegrationTest.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/domain/Domain.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/domain/Repositories.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/IngestionWorker.kt`

**Interfaces:**
- Produces: `ExternalAccess`, `ConnectorFailure`, `ConnectorCheckpoint`, `ConnectorBatch`, expanded `SourceDocument`
- Produces: `PermissionSyncAttemptEntity` and repository

- [ ] **Step 1: Write failing connector model tests**

```kotlin
@Test fun connectorFailureHasExactlyOneTypedTarget()
@Test fun externalAccessRepresentsPublicAndPrivateDocuments()
@Test fun checkpointJsonRoundTripsWithoutLosingHasMore()
```

Assert document and entity failures retain only their typed target fields.

- [ ] **Step 2: Run the model tests and verify compilation failure**

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test \
  --tests com.onyx.foss.kotlin.ingestion.ConnectorModelsTest
```

Expected: types do not exist.

- [ ] **Step 3: Add the connector models**

Use these signatures:

```kotlin
data class ExternalAccess(
    val externalUserEmails: Set<String> = emptySet(),
    val externalUserGroupIds: Set<String> = emptySet(),
    val isPublic: Boolean,
) {
    val numEntries: Int get() = externalUserEmails.size + externalUserGroupIds.size
}

sealed interface FailureTarget {
    data class Document(val id: String, val link: String? = null) : FailureTarget
    data class Entity(val id: String, val missedStart: Instant? = null, val missedEnd: Instant? = null) : FailureTarget
}

data class ConnectorFailure(val target: FailureTarget, val message: String, val errorType: String? = null)
data class ConnectorCheckpoint(val value: JsonNode, val hasMore: Boolean)
data class ConnectorBatch(
    val documents: List<SourceDocument> = emptyList(),
    val failures: List<ConnectorFailure> = emptyList(),
    val checkpoint: ConnectorCheckpoint,
)
```

Move `SourceDocument` from `IngestionWorker.kt` and add `externalAccess: ExternalAccess? = null`.

- [ ] **Step 4: Write the failing V2 schema test**

Assert these columns and tables exist:

```text
indexed_documents.external_access
ingestion_errors.entity_id
ingestion_errors.failed_time_range_start
ingestion_errors.failed_time_range_end
permission_sync_attempts
```

- [ ] **Step 5: Add the V2 migration**

Use PostgreSQL-native definitions:

```sql
ALTER TABLE indexed_documents
    ADD COLUMN external_access JSONB;

ALTER TABLE ingestion_errors
    ADD COLUMN entity_id VARCHAR(2048),
    ADD COLUMN failed_time_range_start TIMESTAMPTZ,
    ADD COLUMN failed_time_range_end TIMESTAMPTZ;

CREATE TABLE permission_sync_attempts (
    id BIGSERIAL PRIMARY KEY,
    cc_pair_id BIGINT NOT NULL REFERENCES connector_credential_pairs(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL DEFAULT 'NOT_STARTED',
    error_msg TEXT,
    time_started TIMESTAMPTZ,
    time_finished TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE connector_credential_pairs
    ADD COLUMN last_pruned_at TIMESTAMPTZ;

CREATE UNIQUE INDEX uq_permission_sync_attempt_active
    ON permission_sync_attempts(cc_pair_id)
    WHERE status IN ('NOT_STARTED', 'IN_PROGRESS');
```

- [ ] **Step 6: Add JPA fields, entity, and repository**

Map `external_access` with `@JdbcTypeCode(SqlTypes.JSON)`.
Reuse `AttemptStatus` for permission attempt status values.

- [ ] **Step 7: Run model and schema tests**

Run both Task 3 test classes.

Expected: PASS on PostgreSQL 15.2.

- [ ] **Step 8: Commit the domain contract**

```bash
git add _kotlin/backend/src/main _kotlin/backend/src/test
git commit -m "feat: add connector batch and permission models"
```

---

### Task 4: Batch 수집, 상태 복구, pruning

**Files:**
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/IngestionWorker.kt`
- Create: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/PruningService.kt`
- Create: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/IngestionScheduler.kt`
- Create: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/RemoteJsonClient.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/RemoteConnectorLoaders.kt`
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/IngestionProcessorIntegrationTest.kt`
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/PruningServiceTest.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/domain/Repositories.kt`
- Modify: `backend/PYTHON_PARITY.md`

**Interfaces:**
- Consumes: `Sequence<ConnectorBatch>` and Task 3 domain types
- Produces: batch-safe checkpoint persistence, selective error resolution, repeated-error calculation, safe pruning

- [ ] **Step 1: Write failing batch lifecycle tests**

Create fake batches in the test with two checkpoints.

```kotlin
@Test fun savesCheckpointAfterEachSuccessfulBatch()
@Test fun doesNotSaveFailedBatchCheckpoint()
@Test fun documentFailureFinishesCompletedWithErrors()
@Test fun fatalFailureFinishesFailed()
@Test fun successfulDocumentResolvesOnlyItsOwnPriorErrors()
```

Assert attempt, job, pair, checkpoint, document, and error rows.

- [ ] **Step 2: Run tests and verify current failures**

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test \
  --tests com.onyx.foss.kotlin.ingestion.IngestionProcessorIntegrationTest
```

Expected: current processor accepts a `List`, marks all prior errors resolved, and cannot represent partial success.

- [ ] **Step 3: Change loaders and processor to synchronous batches**

Change these signatures:

```kotlin
fun FileDocumentLoader.load(config: JsonNode?): Sequence<ConnectorBatch>
fun RemoteConnectorLoaders.load(
    source: ConnectorSource,
    config: JsonNode?,
    credentials: JsonNode,
    checkpoint: JsonNode?,
): Sequence<ConnectorBatch>
```

Keep processing synchronous. Save each batch checkpoint only after all its documents and failures are persisted.

Move `RemoteJsonClient` unchanged into its own file. Remove the old declaration from `RemoteConnectorLoaders.kt`.

- [ ] **Step 4: Add repeated-error tests and calculation**

```kotlin
@Test fun scheduledConnectorNeedsFiveConsecutiveFailures()
@Test fun manualConnectorNeedsOneFailure()
@Test fun successfulAttemptClearsRepeatedErrorState()
```

Add one service function:

```kotlin
internal fun isRepeatedError(refreshFreq: Long?, recent: List<IngestionAttemptEntity>): Boolean {
    val required = if (refreshFreq == null) 1 else 5
    return recent.take(required).size == required && recent.take(required).all { it.status == AttemptStatus.FAILED }
}
```

- [ ] **Step 5: Write failing pruning tests**

```kotlin
@Test fun deletesDocumentsMissingFromCompleteEnumeration()
@Test fun keepsDocumentWhoseRetrievalReturnedFailure()
@Test fun leavesDatabaseRowsWhenOpenSearchDeletionFails()
@Test fun doesNotPruneAfterIncrementalCheckpointRun()
```

- [ ] **Step 6: Implement `PruningService`**

Use set difference only after complete enumeration:

```kotlin
val protectedIds = seenDocumentIds + failedDocumentIds
val removedIds = documents.findSourceIdsByCcPairId(pairId).toSet() - protectedIds
if (removedIds.isNotEmpty()) {
    indexer.deleteDocuments(pairId, removedIds)
    documents.deleteByCcPairIdAndSourceDocumentIdIn(pairId, removedIds)
}
```

Call OpenSearch before deleting DB rows.

- [ ] **Step 7: Add concurrent claim test**

Start two transactions against one queued job.
Assert only one `claimNext()` call returns its ID.

- [ ] **Step 8: Add refresh and pruning schedule tests**

```kotlin
@Test fun queuesIncrementalRunWhenRefreshFrequencyIsDue()
@Test fun doesNotQueueOverlappingRun()
@Test fun doesNotQueuePausedPair()
@Test fun queuesFromBeginningRunWhenPruneFrequencyIsDue()
```

Implement `IngestionScheduler` with one DB query for pairs and nonterminal jobs.
Use `refreshFreq` for incremental jobs and `pruneFreq` for full jobs.
Set `lastPrunedAt` only after pruning succeeds.

- [ ] **Step 9: Run ingestion and pruning tests**

Expected: PASS with real PostgreSQL lock semantics.

- [ ] **Step 10: Mark covered core rows and commit**

```bash
git add _kotlin/backend/src/main _kotlin/backend/src/test _kotlin/backend/PYTHON_PARITY.md
git commit -m "feat: process connector batches safely"
```

---

### Task 5: File 커넥터 동작

**Files:**
- Create: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/FileConnectorLoader.kt`
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/FileConnectorLoaderTest.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/IngestionWorker.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/service/FileStorageService.kt`
- Modify: `backend/PYTHON_PARITY.md`

**Interfaces:**
- Consumes: `Sequence<ConnectorBatch>`
- Produces: Python-compatible File documents and metadata

- [ ] **Step 1: Port the five Python File scenarios as failing tests**

Create Kotlin tests corresponding to:

```text
test_single_text_file_with_metadata
test_two_text_files_with_zip_metadata
test_tabular_file_sets_file_id_on_document
test_non_tabular_file_leaves_file_id_none
test_mixed_batch_only_tabular_gets_file_id
```

Use test resources created inside `@TempDir`. Assert document IDs, titles, content, metadata, and file IDs.

- [ ] **Step 2: Run File tests and verify failures**

Expected: zip metadata and tabular file identity assertions fail.

- [ ] **Step 3: Move File loading into `FileConnectorLoader`**

Return one `ConnectorBatch` with `hasMore=false`.
Reuse Tika for supported non-tabular formats.
Use the stored asset ID as the stable source ID.
Remove the old `FileDocumentLoader` declaration from `IngestionWorker.kt`.

- [ ] **Step 4: Implement zip metadata association**

Read the existing upload contract fields `file_locations`, `file_names`, and `zip_metadata_file_id`.
Do not introduce another metadata format.

- [ ] **Step 5: Implement tabular file identity**

Set `metadata["file_id"]` only for tabular input.
Keep non-tabular metadata unchanged.

- [ ] **Step 6: Run File and management tests**

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test \
  --tests com.onyx.foss.kotlin.ingestion.FileConnectorLoaderTest \
  --tests com.onyx.foss.kotlin.api.AdminApiIntegrationTest
```

Expected: PASS.

- [ ] **Step 7: Mark File rows covered and commit**

```bash
git add _kotlin/backend/src _kotlin/backend/PYTHON_PARITY.md
git commit -m "feat: complete Kotlin file connector behavior"
```

---

### Task 6: Jira 커넥터 동작

**Files:**
- Create: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/JiraConnectorLoader.kt`
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/JiraConnectorLoaderTest.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/RemoteConnectorLoaders.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/RemoteJsonClient.kt`
- Modify: `backend/PYTHON_PARITY.md`

**Interfaces:**
- Consumes: `ConnectorBatch`, `ExternalAccess`, `RemoteJsonClient`
- Produces: `Sequence<ConnectorBatch>` with `JiraCheckpoint`

- [ ] **Step 1: Define and test the Jira checkpoint**

Use this shape:

```kotlin
data class JiraCheckpoint(
    val hasMore: Boolean = true,
    val allIssueIds: List<List<String>> = emptyList(),
    val idsDone: Boolean = false,
    val cursor: String? = null,
    val offset: Int? = null,
    val seenHierarchyNodeIds: Set<String> = emptySet(),
)
```

Test JSON round-trip and cursor resume.

- [ ] **Step 2: Port Jira validation and query tests**

Port the applicable scenarios from:

```text
test_jira_basic.py
test_jira_checkpointing.py
test_jira_error_handling.py
test_jira_bulk_fetch.py
test_jira_large_ticket_handling.py
test_jira_slim_retrieval.py
test_jira_permission_sync.py
```

Create explicit Kotlin test methods for project JQL, custom JQL, scoped token, skipped label, batch split, large issue, typed 401/403/404/429 errors, and permissions.

- [ ] **Step 3: Run Jira tests and verify failures**

Expected: cursor, validation, partial failure, permission, and typed error tests fail.

- [ ] **Step 4: Implement Jira page batches**

Support Jira Cloud cursor pagination and Server/Data Center offset pagination.
Yield one batch per API page.
Return the updated `JiraCheckpoint` with every batch.

- [ ] **Step 5: Implement Jira document conversion and failures**

Preserve summary, description, comments allowed by config, updated time, labels, links, and hierarchy IDs.
Convert one bad issue into `ConnectorFailure` without discarding the page's good issues.

- [ ] **Step 6: Implement Jira-specific validation and rate-limit behavior**

Map Python's typed validation cases to stable `IllegalArgumentException` messages.
Do not add generic retry behavior to `RemoteJsonClient`.

- [ ] **Step 7: Implement Jira permission retrieval**

Populate `ExternalAccess` from project and issue permissions.
Keep group IDs unprefixed for permission sync output and prefixed where the Python indexing path does so.

- [ ] **Step 8: Run all Jira tests**

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test \
  --tests com.onyx.foss.kotlin.ingestion.JiraConnectorLoaderTest
```

Expected: PASS.

- [ ] **Step 9: Mark Jira rows and commit**

```bash
git add _kotlin/backend/src _kotlin/backend/PYTHON_PARITY.md
git commit -m "feat: complete Kotlin Jira connector behavior"
```

---

### Task 7: Confluence 커넥터 동작

**Files:**
- Create: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/ConfluenceConnectorLoader.kt`
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/ConfluenceConnectorLoaderTest.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/RemoteConnectorLoaders.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/RemoteJsonClient.kt`
- Modify: `backend/PYTHON_PARITY.md`

**Interfaces:**
- Consumes: `ConnectorBatch`, `ExternalAccess`, Tika HTML parsing, `RemoteJsonClient`
- Produces: `Sequence<ConnectorBatch>` with `ConfluenceCheckpoint`

- [ ] **Step 1: Define and test the Confluence checkpoint**

```kotlin
data class ConfluenceCheckpoint(val hasMore: Boolean = true, val nextPageUrl: String? = null)
```

Test first page, resumed next link, and completion.

- [ ] **Step 2: Port basic, checkpoint, and HTML scenarios**

Port applicable tests from:

```text
test_confluence_basic.py
test_confluence_html_parser.py
test_confluence_checkpointing.py
test_extract_text.py
test_include_attachments_skip.py
test_slim_doc_image_skip.py
```

Assert page text, links, tables, date lozenges, image policy, CQL, and checkpoint progression.

- [ ] **Step 3: Port attachment and resolver scenarios**

Port applicable tests from:

```text
test_attachment_pagination_400.py
test_confluence_attachment_links.py
test_confluence_resolver.py
```

Assert partial attachment preservation, targeted refetch, platform-specific links, and `ConnectorFailure` IDs.

- [ ] **Step 4: Port pagination, retry, and permission scenarios**

Port applicable tests from:

```text
test_onyx_confluence.py
test_confcloud_77618_fallback.py
test_confluence_permissions_basic.py
test_confluence_user_email_overrides.py
test_rate_limit_handler.py
```

Exclude only `test_per_ancestor_shim_resolves_to_ee_implementation`.
Assert Cloud and Server limit reduction, 429 `Retry-After`, 500/504 fallback, space permission, page restriction, and cache isolation.

- [ ] **Step 5: Run Confluence tests and verify failures**

Expected: attachments, comments, HTML, retry, Server pagination, and permission tests fail.

- [ ] **Step 6: Implement page and attachment batches**

Yield page results incrementally.
Fetch comments and attachments only when enabled.
Use Tika's HTML parser and a focused SAX handler; do not add another HTML dependency.

- [ ] **Step 7: Implement Confluence-specific fallback rules**

Honor `Retry-After` on 429.
Reduce Cloud and Server page sizes exactly where Python does.
Implement the CONFCLOUD-77618 per-page permission fallback without EE code.

- [ ] **Step 8: Implement permission retrieval**

Combine space access, page restriction, ancestor restriction, and user email override results into `ExternalAccess`.
Use private empty access when Python cannot determine a document's permissions safely.

- [ ] **Step 9: Run all Confluence tests**

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test \
  --tests com.onyx.foss.kotlin.ingestion.ConfluenceConnectorLoaderTest
```

Expected: PASS.

- [ ] **Step 10: Mark Confluence rows and commit**

```bash
git add _kotlin/backend/src _kotlin/backend/PYTHON_PARITY.md
git commit -m "feat: complete Kotlin Confluence connector behavior"
```

---

### Task 8: GitHub 커넥터 동작

**Files:**
- Create: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/GithubConnectorLoader.kt`
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/GithubConnectorLoaderTest.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/RemoteConnectorLoaders.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/RemoteJsonClient.kt`
- Modify: `backend/PYTHON_PARITY.md`

**Interfaces:**
- Consumes: `ConnectorBatch`, `ExternalAccess`, `RemoteJsonClient`
- Produces: `Sequence<ConnectorBatch>` with staged `GithubCheckpoint`

- [ ] **Step 1: Define and test the GitHub checkpoint**

Model repository ID, stage, page cursor, branch, and `hasMore`.
Test JSON round-trip, cursor resume, cursor expiration, and branch change.

- [ ] **Step 2: Port checkpoint and validation tests**

Port all applicable scenarios from `test_github_checkpointing.py`.
Assert empty repo, PR-only, issue-only, cursor fallback, cursor completion, rate reset, and typed validation errors.

- [ ] **Step 3: Port file tests**

Port all applicable scenarios from `test_github_files.py`.
Assert extension and size filtering, binary failure, undecodable failure, truncated tree failure, branch URL, and stage progression.

- [ ] **Step 4: Port basic and slim permission tests**

Port `test_github_basic.py` and applicable `test_github_slim_connector.py` scenarios.
Assert PR filtering from issue results and permission-populated slim documents.

- [ ] **Step 5: Run GitHub tests and verify failures**

Expected: checkpoint stage, cursor, branch, error, and permission tests fail.

- [ ] **Step 6: Implement staged GitHub batches**

Process repositories one at a time through file, PR, and issue stages.
Save checkpoint state after each API page.
Do not cap files with the current arbitrary `take(500)` limit.

- [ ] **Step 7: Implement GitHub rate-limit behavior**

Read the API rate reset value after a rate-limit response.
Wait until reset using an injected sleeper in tests.
Do not apply this policy to Jira or Confluence.

- [ ] **Step 8: Implement repository permissions**

Populate public state, user emails, and team IDs in `ExternalAccess`.
Preserve permission data on PRs, issues, and files.

- [ ] **Step 9: Run all GitHub tests**

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test \
  --tests com.onyx.foss.kotlin.ingestion.GithubConnectorLoaderTest
```

Expected: PASS.

- [ ] **Step 10: Mark GitHub rows and commit**

```bash
git add _kotlin/backend/src _kotlin/backend/PYTHON_PARITY.md
git commit -m "feat: complete Kotlin GitHub connector behavior"
```

---

### Task 9: Full document permission sync

**Files:**
- Create: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/PermissionSyncWorker.kt`
- Create: `backend/src/test/kotlin/com/onyx/foss/kotlin/ingestion/PermissionSyncIntegrationTest.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/api/AdminController.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/service/IngestionQueryService.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/IngestionWorker.kt`
- Modify: `backend/src/main/kotlin/com/onyx/foss/kotlin/domain/Repositories.kt`
- Modify: `backend/PYTHON_PARITY.md`

**Interfaces:**
- Consumes: connector permission batches from Tasks 6 through 8
- Produces: persisted ACLs and real `/permission-sync-attempts` responses
- Keeps: `/external-group-sync-attempts` as `applicable=false`

- [ ] **Step 1: Write failing permission attempt route tests**

Port applicable cases from `test_cc_pair_sync_attempts_routes.py`:

```kotlin
@Test fun permissionAttemptsReturnApplicableTrueForSupportedConnector()
@Test fun permissionAttemptsAreNewestFirstAndPaginated()
@Test fun permissionAttemptsExposeFailedAndPartialStates()
@Test fun externalGroupAttemptsRemainNotApplicable()
```

- [ ] **Step 2: Write failing permission worker tests**

```kotlin
@Test fun storesAclForEveryReturnedDocument()
@Test fun fileAclUsesThePairPublicAccessFlag()
@Test fun failedPermissionLookupStoresPrivateAclAndPartialStatus()
@Test fun fatalPermissionFailureLeavesPreviousAclUntouched()
@Test fun duplicateDocumentUpsertIsIdempotent()
```

- [ ] **Step 3: Run tests and verify current failures**

Expected: route returns `applicable=false` and no worker exists.

- [ ] **Step 4: Implement permission attempt query responses**

Return this stable shape:

```kotlin
mapOf(
    "applicable" to true,
    "items" to attempts,
    "total_items" to total,
)
```

Keep the external-group route unchanged.

- [ ] **Step 5: Implement synchronous permission processing**

For each supported pair:

1. Create `NOT_STARTED` attempt.
2. Mark it `IN_PROGRESS`.
3. Consume connector permission batches.
4. Upsert `indexed_documents.external_access` by source document ID.
5. Update OpenSearch ACL fields idempotently.
6. Finish as `SUCCESS`, `COMPLETED_WITH_ERRORS`, or `FAILED`.

Invoke permission processing after each terminal successful or partially successful indexing run.
The partial unique index prevents a second active permission attempt for the same pair.

- [ ] **Step 6: Run permission tests**

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test \
  --tests com.onyx.foss.kotlin.ingestion.PermissionSyncIntegrationTest
```

Expected: PASS.

- [ ] **Step 7: Mark permission rows and commit**

```bash
git add _kotlin/backend/src _kotlin/backend/PYTHON_PARITY.md
git commit -m "feat: add FOSS document permission sync"
```

---

### Task 10: 전체 stack 검증과 문서 정리

**Files:**
- Create: `scripts/test-kotlin-file-ingestion.sh`
- Modify: `README.md`
- Modify: `SOURCE_PROVENANCE.md`
- Modify: `backend/PYTHON_PARITY.md`

**Interfaces:**
- Consumes: all prior tasks and running Docker Compose services
- Produces: one repeatable Web-to-OpenSearch verification command

- [ ] **Step 1: Write the failing live File ingestion script**

The script must:

1. POST one text file through `http://localhost:3000/api/manage/admin/connector/file/upload`.
2. Create and associate a File connector through Web API routes.
3. Poll its index attempt until a terminal status.
4. Query PostgreSQL for the indexed document row.
5. Query OpenSearch for the matching `cc_pair_id` and source document ID.
6. Exit nonzero if any expected value is missing.

Use `curl`, `jq`, `docker compose exec -T postgres psql`, and `docker compose exec -T opensearch curl`.
Do not add another scripting dependency.

- [ ] **Step 2: Run the script and verify failure before final fixes**

```bash
./scripts/test-kotlin-file-ingestion.sh
```

Expected: any remaining API, worker, model server, or OpenSearch contract difference fails visibly.

- [ ] **Step 3: Fix only failures exposed by the live flow**

Do not add new connector or user behavior.
Keep each fix at the shared root cause used by all callers.

- [ ] **Step 4: Run the complete backend suite**

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test
```

Expected: PASS.

- [ ] **Step 5: Run focused repository checks**

```bash
pre-commit run --files \
  _kotlin/backend/build.gradle.kts \
  $(git diff --name-only --diff-filter=ACMR 3fbfd7650...HEAD -- _kotlin/backend _kotlin/scripts _kotlin/README.md _kotlin/SOURCE_PROVENANCE.md)
```

Expected: PASS.

- [ ] **Step 6: Verify parity matrix completion**

```bash
rg '\| planned \|' backend/PYTHON_PARITY.md
```

Expected: no output. Every row is `covered` or has an approved `excluded` reason.

- [ ] **Step 7: Update docs**

In `README.md`, remove connector limits that this work eliminated.
In `SOURCE_PROVENANCE.md`, list the exact FOSS source paths used by each loader.
Do not mention Enterprise implementation details.

- [ ] **Step 8: Run the live flow again**

```bash
./scripts/test-kotlin-file-ingestion.sh
```

Expected: PASS with one document in PostgreSQL and OpenSearch.

- [ ] **Step 9: Commit final verification**

```bash
git add _kotlin/scripts/test-kotlin-file-ingestion.sh _kotlin/README.md \
  _kotlin/SOURCE_PROVENANCE.md _kotlin/backend/PYTHON_PARITY.md
git commit -m "test: verify Kotlin ingestion end to end"
```

---

## Final Verification

Run these commands from `_kotlin`:

```bash
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test
cd ..
./scripts/test-kotlin-file-ingestion.sh
docker compose config -q
git status --short
```

Success requires:

- All Gradle tests pass.
- The live File ingestion script passes.
- Docker Compose configuration validates.
- `PYTHON_PARITY.md` contains no `planned` row.
- No source outside the approved FOSS boundary appears in provenance.
