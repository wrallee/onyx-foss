# Source provenance

This directory is derived only from the FOSS source in the parent checkout.
The parent checkout's MIT `LICENSE` is reproduced in this directory.

## Allowed sources

| Destination | Allowed local source | Use |
| --- | --- | --- |
| `web/` | `../web/` | Admin UI, design system, icons, translations, and static assets |
| `backend/` | `../backend/onyx/connectors/{file,jira,confluence,github}` and related FOSS API models | Behavioral reference for the Kotlin port |
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
