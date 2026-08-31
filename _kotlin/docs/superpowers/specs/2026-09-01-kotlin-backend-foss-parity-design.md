# Kotlin Backend FOSS Parity Design

## Context

The Kotlin backend copies many Onyx FOSS API shapes but omits substantial behavior.
Current tests mostly cover response shapes and successful remote API calls.
They do not prove database behavior, ingestion recovery, pruning, or connector parity.

The parent Python checkout already implements and tests the required behavior.
This design uses that implementation as the behavioral source of truth.

## Goal

Restore applicable FOSS behavior within the Kotlin backend's existing feature families.
Add tests that detect future differences from the Python behavior.

API similarity alone is not sufficient.
Database state, checkpoints, errors, indexed documents, and permission metadata must also match.

## Scope

### Included

- Credential, Connector, CC Pair, Document Set, and file administration
- File, Jira, Confluence, and GitHub connectors
- Connector validation, pagination, checkpoints, partial failures, and rate limits
- Ingestion jobs, attempts, status transitions, retries, and recovery
- Embedding requests and OpenSearch writes and deletions
- Pruning and safe handling of failed document retrieval
- Full document permission synchronization
- Permission synchronization attempts and API contracts
- Existing Web contracts for these features

### Excluded

- Connector types without a Kotlin source implementation and active entry point
- User registration, authentication, sessions, and user-specific settings
- Enterprise features and all Enterprise source code
- Multitenancy
- External group synchronization
- SAML, LDAP, OIDC, and SCIM integration

## Source Boundary

Use sources in this order:

1. Parent Python FOSS implementation behavior
2. Parent Python FOSS test expectations
3. Existing Web request and response contracts
4. Current Kotlin documentation

The Python implementation wins when the Kotlin README describes an incomplete port.
Authentication, Enterprise, multitenancy, and external group synchronization remain excluded.

Do not use code from an `ee/` directory or any Enterprise-licensed source.
Record source provenance for ported behavior and copied fixtures.

## Feature Boundary

Increase depth without increasing connector breadth.

For an implemented connector, restore missing behavior from its Python implementation.
Do not add another connector because its Python tests are available.

Add shared behavior only when an included connector or management flow requires it.
Do not create speculative compatibility infrastructure.

## Architecture

### Management Lifecycle

The management layer owns these operations:

- Credential creation, masking, update, association checks, and deletion
- Connector creation, update, association, pause, run, and deletion
- CC Pair metadata, status, indexing summaries, and attempt history
- Document Set membership, update, deletion, and public behavior
- File upload, replacement, removal, metadata, and connector updates

Use the existing Spring controllers and services where their boundaries remain suitable.
Change them only when an approved behavior cannot fit the current structure.

### Synchronous Connector Batches

The Python connectors use synchronous generators.
The Kotlin equivalent will use a synchronous `Sequence`.

Each yielded connector batch carries:

- Documents
- Connector failures
- The next connector checkpoint
- Whether more work remains

File ingestion can yield one batch.
Remote connectors yield API page or checkpoint units.

Do not introduce coroutine `Flow` or an asynchronous connector framework.

### Ingestion Flow

The ingestion worker processes one connector batch at a time.

1. Claim one eligible job with PostgreSQL locking.
2. Mark its attempt in progress.
3. Load one connector batch.
4. Transform, chunk, embed, and index its documents.
5. Save document failures with their identifiers and context.
6. Save the checkpoint at the same safe boundary as Python.
7. Continue until the connector reports completion.
8. Apply Python-compatible pruning after complete enumeration.
9. Set the final attempt and CC Pair states.

The worker must not load every remote document into memory before processing.

### Error Semantics

Do not create one generic retry policy for all connectors.
Port each included connector's Python behavior.

- A document-level `ConnectorFailure` produces a partial result.
- Partial results can finish as `COMPLETED_WITH_ERRORS`.
- An unhandled fatal error finishes the attempt as `FAILED`.
- Successfully processed documents resolve only their applicable prior errors.
- Repeated-error state follows the Python consecutive-failure rule.
- A failed retrieval preserves its document identifier during pruning.
- Checkpoints advance only at the Python-compatible safe boundary.
- Embedding and index failures remain document-level when Python treats them that way.

Rate-limit behavior also remains connector-specific.
GitHub, Confluence, and Jira must follow their own Python policies.

Never expose credential values through API responses, logs, or exception messages.

### Document Permission Synchronization

Document permission synchronization is included because it belongs to FOSS connector behavior.

It must collect and store supported external document access metadata.
It must track permission synchronization attempts and failures.
It must avoid pruning documents after a temporary permission retrieval failure.

No current user exists to consume these access rules.
Search-time user enforcement is deferred until identity support exists.

External group synchronization remains excluded.
Its Onyx execution implementation belongs to Enterprise code.

## Test Strategy

### Fast Tests

Use JUnit for deterministic validation, conversion, and state calculations.
Use existing helpers and dependencies before adding new utilities.

### Connector Contract Tests

Use MockWebServer for remote connector tests.
Port every applicable Python scenario for the four included connectors.

These tests cover pagination, authentication headers, validation, checkpoints, failures, and rate limits.
They must also cover connector-specific permission retrieval.

Actual Jira, Confluence, and GitHub accounts are not required for normal test execution.
Optional live smoke tests can use real credentials outside the required suite.

### PostgreSQL Integration Tests

Use an isolated PostgreSQL container for database integration tests.
Run the real Flyway migrations.

Do not use H2 for database integration tests.
The schema uses PostgreSQL JSONB, casts, timestamps, and identity behavior.
Job claims also require `FOR UPDATE SKIP LOCKED` semantics.

The integration suite covers CRUD, foreign keys, uniqueness, transactions, pagination, and concurrent job claims.

### Full Ingestion Tests

Use Docker Compose for a representative full File ingestion flow.
The flow includes PostgreSQL, the model server, and OpenSearch.

Send backend requests through the Web service.
Verify the final API state, database rows, and OpenSearch documents.

Add more full-stack cases only when smaller tests cannot prove the behavior.

## Scenario Inventory

| Area | Required scenario groups |
| --- | --- |
| Administration | Create, update, associate, reject invalid association, pause, run, and delete |
| File | Multiple files, metadata, replacement, removal, parsing, checkpoint completion, and pruning |
| Jira | Project scope, JQL, pagination, checkpoints, skipped issues, typed errors, rates, and permissions |
| Confluence | Cloud and Server behavior, pages, attachments, comments, HTML, checkpoints, rates, and permissions |
| GitHub | Public and private repositories, branches, issues, pull requests, files, checkpoints, rates, and permissions |
| Ingestion | Claiming, state transitions, partial failures, fatal failures, recovery, checkpoints, and repeated errors |
| Pruning | Missing documents, retrieval failures, index deletion failures, and database consistency |
| Permission sync | Attempt states, partial failures, stored ACL data, and safe retries |
| API contracts | Response fields, status values, pagination, validation errors, and Web compatibility |

Before implementation, map each Kotlin scenario to its Python source test or implementation path.
Remove only scenarios that depend on an explicitly excluded feature.

## Acceptance Criteria

- Every applicable Python FOSS scenario has a Kotlin test or documented equivalent coverage.
- Tests expose missing Kotlin behavior before implementation changes.
- All fast and connector contract tests pass.
- All PostgreSQL integration tests pass against real PostgreSQL.
- The representative Docker File ingestion test passes through the Web service.
- Checkpoints, partial failures, recovery, and pruning match Python behavior.
- Supported connectors collect and store document ACLs with complete attempt tracking.
- No new connector type is added.
- No Enterprise code or fixture is copied or translated.
- Existing supported Web administration flows continue to work.

## Deferred Work

User identity and search-time ACL enforcement remain deferred.
Future SAML or LDAP work must use MIT FOSS code and public standards only.
It must not derive from Onyx Enterprise source code.
