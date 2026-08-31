# Source provenance

This directory is derived only from the FOSS source in the parent checkout.
The parent checkout's MIT `LICENSE` is reproduced in this directory.

## Allowed sources

| Destination | Exact FOSS source paths | Use |
| --- | --- | --- |
| `web/` | `../web/` | Admin UI, design system, icons, translations, and static assets |
| `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/FileConnectorLoader.kt` | `../backend/onyx/connectors/file/connector.py` | File document IDs, metadata, and load behavior |
| `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/JiraConnectorLoader.kt` | `../backend/onyx/connectors/jira/connector.py`, `../backend/onyx/connectors/jira/access.py`, `../backend/onyx/connectors/jira/utils.py` | Jira validation, polling, conversion, and access behavior |
| `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/ConfluenceConnectorLoader.kt` | `../backend/onyx/connectors/confluence/connector.py`, `../backend/onyx/connectors/confluence/access.py`, `../backend/onyx/connectors/confluence/models.py`, `../backend/onyx/connectors/confluence/onyx_confluence.py`, `../backend/onyx/connectors/confluence/utils.py` | Confluence collection, attachments, comments, retry, and access behavior |
| `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/GithubConnectorLoader.kt` | `../backend/onyx/connectors/github/connector.py`, `../backend/onyx/connectors/github/models.py`, `../backend/onyx/connectors/github/rate_limit_utils.py`, `../backend/onyx/connectors/github/utils.py` | GitHub discovery, collection, checkpoint, retry, and access behavior |
| `backend/src/main/kotlin/com/onyx/foss/kotlin/ingestion/IngestionWorker.kt` | `../backend/onyx/background/indexing/run_docfetching.py`, `../backend/onyx/background/celery/tasks/docfetching/tasks.py`, `../backend/onyx/db/index_attempt.py` | Batch, pause, attempt, checkpoint, and pruning behavior |
| `backend/src/main/kotlin/com/onyx/foss/kotlin/service/AdminService.kt` | `../backend/onyx/server/documents/cc_pair.py`, `../backend/onyx/db/index_attempt.py`, `../backend/onyx/db/document_set.py` | Connector status, last-indexed, and Document Set behavior |
| `docs/model-server-spike.md` | `../backend/model_server/` and local call sites | Model server feasibility analysis |

`../_kotlinmania/`, external repositories, Enterprise Edition source, and paid
feature source are not allowed inputs.

## License and feature policy

- Preserve upstream notices and file-level headers on copied files.
- Do not disable or bypass license checks.
- Do not force paid feature flags on.
- Unsupported features remain visible only where the copied FOSS UI already
  provides their presentation. The local support policy disables interaction.
- Application authentication is intentionally absent for an internal-only
  deployment. This does not change software licensing or connector credential
  protection.
