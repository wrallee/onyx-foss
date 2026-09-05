# Kotlin 백엔드 Permission Sync / Document ACL 기능 제거 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 미완성 상태로 과도한 외부 API 호출(DDOS 현상) 및 주기적 부하를 유발하는 Permission Sync 및 Document Access Control(ACL) 관련 백엔드 로직을 완전히 제거하고, 문서는 기본 공개(Public) 접근 모델로 단순화한다.

**Architecture:** 프론트엔드 UI(`web/`)는 상위 FOSS 코드베이스와 동일하게 유지하고, 커넥터 페어는 항상 공개(`access_type = "public"`)로 운영하여 탭 UI 자체를 렌더링하지 않는다. 컨트롤러 내의 권한 동기화 엔드포인트는 stub 없이 완전히 삭제한다. Jira·Confluence·GitHub 수집기에서 권한 스크래핑/동기화 로직을 완전히 걷어내고, 백그라운드 `PermissionSyncWorker` 및 관련 스케줄러를 삭제한다. DB 스키마는 V16 마이그레이션으로 잔존 테이블(`permission_sync_staging`, `permission_sync_attempts`)을 DROP하여 완전히 정리한다.

**Tech Stack:** Kotlin 2.3.20, Java 25, Spring Boot 4.0.7, PostgreSQL, Flyway, OpenSearch

---

## Global Constraints

- 프론트엔드(`_kotlin/web`)의 UI 컴포넌트는 상위 FOSS 원본과 일치하도록 유지하고 불필요한 UI 변경을 하지 않는다.
- Jira, Confluence, GitHub 수집기에서 권한 관련 추가 API 호출(space permissions, page restrictions, collaborator emails, project permission schemes)을 일체 수행하지 않는다.
- 모든 커넥터 페어의 `access_type`은 `"public"`으로 고정하며, 모든 문서는 FOSS 기본인 공개(`ExternalAccess(isPublic = true)`)로 인덱싱한다.
- 컨트롤러 내 권한 동기화 관련 엔드포인트 코드는 stub 없이 완전히 삭제한다.
- Flyway 마이그레이션은 기존 V1~V15의 체크섬을 건드리지 않고 `V16__drop_permission_sync.sql`로 테이블을 DROP한다.
- Watchlist `WL-20260831-001`을 정리하고 관련 문서(`README.md`)를 갱신한다.

---

## Issues to Address

- **외부 서비스 대상 과도한 API 호출 (DDOS 원인)**:
  - `JiraConnectorLoader`: 프로젝트별 permission scheme 조회(`/rest/api/.../permissionscheme`) 및 그룹 전개로 인한 다량의 API 호출.
  - `ConfluenceConnectorLoader`: space 권한 전수 조회(REST/JsonRpc) 및 모든 수집 페이지마다 per-page restriction 조회(`/rest/api/content/.../restriction/...`) 수행.
  - `GithubConnectorLoader`: `GithubStage.PERMISSIONS` 단계에서 collaborator 전원에 대해 개별 `/users/{login}` API를 호출하여 이메일을 수집하고 팀 권한을 조회하는 N+1 호출.
- **주기적 Permission Sync 워커 부하**:
  - `PermissionSyncScheduledWorker`가 5초마다 주기적으로 DB 락 및 claim을 걸며 외부 로더를 호출하고 OpenSearch `updateByQuery`를 실행함.
- **불필요한 DB 테이블, 엔티티, 컨트롤러 코드 잔존**:
  - `permission_sync_attempts`, `permission_sync_staging` 테이블 및 JPA Entity, Repository.
  - `AdminController`의 `permission-sync-attempts`, `external-group-sync-attempts` 엔드포인트.
- **미정리된 Watchlist 및 문서**:
  - `_kotlin/.watchlist/WATCHLIST.md`의 `WL-20260831-001` 항목 및 `_kotlin/README.md`의 "permission sync" 지원 문구.

---

## Important Notes

- 프론트엔드(`web/src/app/admin/connector/[ccPairId]/page.tsx`)는 `ccPair.access_type === "sync"`일 때만 권한 탭(`SyncAttemptsTabs`)을 렌더링하고 엔드포인트를 호출함.
- `access_type`이 `"public"`이면 일반 인덱싱 테이블(`IndexAttemptsTable`)만 렌더링되므로, `access_type`을 `"public"`으로 고정하면 컨트롤러 엔드포인트를 완전히 삭제해도 프론트엔드가 해당 API를 전혀 호출하지 않음.
- OpenSearch 청크의 `external_user_emails` 및 `external_user_group_ids` 필드는 기존 인덱스 매핑과의 호환성을 유지하기 위해 빈 배열(`[]`)로 저장.
- `OpenSearchIndexer.updateAccess` 메서드는 `PermissionSyncWorker` 전용이었으므로 완전히 삭제.

---

## Implementation strategy

1. **Connector Loaders 권한 스크래핑 로직 제거**:
   - `JiraConnectorLoader.kt`: `projectAccess`, `permissionscheme` 호출, `includePermissions` 관련 분기 제거.
   - `ConfluenceConnectorLoader.kt`: `allSpacePermissions`, `supportsRestSpacePermissions`, `resolveSlimPermission`, space/page restrictions 관련 분기 및 예외 클래스 제거.
   - `GithubConnectorLoader.kt`: `GithubStage.PERMISSIONS` 단계 및 collaborator/team email 조회 로직 제거.
2. **PermissionSyncWorker 및 스케줄러 삭제**:
   - `PermissionSyncWorker.kt` 파일 전체 삭제 (ScheduledWorker, Worker, ClaimService 포함).
3. **Admin Controller & Service 완전 정리**:
   - `AdminController.kt`: `permission-sync-attempts`, `external-group-sync-attempts` 엔드포인트 완전 삭제.
   - `IngestionQueryService.kt`: `permissionAttempts` 메서드 삭제 및 `PermissionSyncAttemptRepository` 의존성 제거.
   - `AdminService.kt`: `PermissionSyncAttemptRepository` 의존성 제거, ccPair의 `accessType`을 `"public"`으로 처리, `last_permission_sync_attempt_*` 필드 제거.
4. **Domain Entity, Repository, Flyway 마이그레이션 정리 (방안 B)**:
   - `V16__drop_permission_sync.sql` 작성 (`DROP TABLE IF EXISTS permission_sync_staging; DROP TABLE IF EXISTS permission_sync_attempts;`).
   - `Domain.kt` & `Repositories.kt`: `PermissionSyncAttemptEntity`, `PermissionSyncStageEntity` 및 관련 리포지토리 완전 삭제.
5. **OpenSearchIndexer 정리**:
   - `OpenSearchIndexer.kt`: `updateAccess` 메서드 완전 삭제.
6. **Watchlist 및 README 갱신**:
   - `_kotlin/.watchlist/WATCHLIST.md`: `WL-20260831-001` 항목 제거.
   - `_kotlin/README.md`: `permission sync` 문구 삭제/정리.
7. **테스트 코드 정리 및 빌드 검증**:
   - 권한 동기화 전용 테스트(`PermissionSyncIntegrationTest.kt`, `ConfluencePermissionSyncIntegrationTest.kt`) 삭제.
   - 기존 통합/단위 테스트(`MigrationSmokeTest.kt`, `AdminApiIntegrationTest.kt`, `AdminDeletionIntegrationTest.kt`, `GithubConnectorLoaderTest.kt`, `JiraConnectorLoaderTest.kt`, `OpenSearchIndexerTest.kt`, `OpenSearchIndexerIntegrationTest.kt`, `SchemaV2IntegrationTest.kt`, `SchemaV4IntegrationTest.kt`)에서 권한 관련 검증 정리 및 통과 확인.
   - 전체 오프라인 컴파일 및 테스트 통과 검증.

---

## Tasks

### Task 1: Watchlist 및 문서 갱신
- [ ] `_kotlin/.watchlist/WATCHLIST.md`에서 `WL-20260831-001` 항목 삭제.
- [ ] `_kotlin/README.md`에서 `permission sync` 지원 범위 문구 제거.

### Task 2: Connector Loader 권한 로직 제거 (DDOS 방지)
- [ ] `JiraConnectorLoader.kt`에서 `projectAccess` 및 `permissionscheme` 조회 로직 제거, 모든 문서 `ExternalAccess(isPublic = true)` 처리.
- [ ] `ConfluenceConnectorLoader.kt`에서 `supportsRestSpacePermissions`, `allSpacePermissions`, `resolveSlimPermission` 등 공간/페이지 권한 조회 로직 및 예외 제거.
- [ ] `GithubConnectorLoader.kt`에서 `GithubStage.PERMISSIONS` 및 collaborator 이메일 순회 조회 로직 제거.
- [ ] 관련 커넥터 단위 테스트(`GithubConnectorLoaderTest.kt`, `JiraConnectorLoaderTest.kt`) 수정.

### Task 3: 백그라운드 PermissionSyncWorker 삭제 및 Controller 엔드포인트 완전 제거
- [ ] `PermissionSyncWorker.kt` 삭제.
- [ ] `AdminController.kt`에서 `permission-sync-attempts` 및 `external-group-sync-attempts` 엔드포인트 완전 삭제.
- [ ] `AdminService.kt` 및 `IngestionQueryService.kt`에서 `PermissionSyncAttemptRepository` 의존성 및 응답 필드 제거.

### Task 4: DB 스키마 마이그레이션(V16) 및 도메인 엔티티 정리
- [ ] `V16__drop_permission_sync.sql` 작성 (`permission_sync_staging`, `permission_sync_attempts` DROP).
- [ ] `Domain.kt` 및 `Repositories.kt`에서 PermissionSync 관련 엔티티/리포지토리 완전 제거.
- [ ] `OpenSearchIndexer.kt`에서 `updateAccess` 완전 제거.

### Task 5: 테스트 코드 정리 및 전체 빌드 검증
- [ ] `PermissionSyncIntegrationTest.kt`, `ConfluencePermissionSyncIntegrationTest.kt` 삭제.
- [ ] `AdminApiIntegrationTest.kt`, `AdminDeletionIntegrationTest.kt`, `MigrationSmokeTest.kt`, `SchemaV2IntegrationTest.kt`, `SchemaV4IntegrationTest.kt`, `OpenSearchIndexerTest.kt`, `OpenSearchIndexerIntegrationTest.kt` 수정.
- [ ] 백엔드 컴파일 및 테스트 실행으로 정상 동작 확인.

---

## Tests

- **Unit / Loader Tests**: `GithubConnectorLoaderTest`, `JiraConnectorLoaderTest`에서 권한 스크래핑 없이 문서 수집 흐름이 정상 작동하는지 검증.
- **Integration Tests**: `MigrationSmokeTest` (V16 마이그레이션 정상 적용 및 테이블 정리 확인), `AdminApiIntegrationTest` (cc-pair 상세 조회 및 커넥터 삭제 시 오류 없이 동작 확인), `OpenSearchIndexerTest`.
- **E2E / Playwright**: `access_type = "public"` 조건에서 일반 인덱싱 테이블만 렌더링되며 프론트엔드가 삭제된 엔드포인트를 호출하지 않음을 확인.
