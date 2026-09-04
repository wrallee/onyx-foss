For Superpowers workflows, save implementation plans under
`docs/superpowers/plans/`, not `plans/`.

# Container Workload Safety

Do not start the complete Compose stack without checking available resources and required services.

Before running `docker compose up`, `docker compose build`, scaling services, or starting integration infrastructure:

- Check available memory, swap, CPU load, disk space, and running containers.
- Resolve the services, profiles, and dependencies that the command will start.
- Inspect declared memory, CPU, and PID limits. Do not assume an unlimited service is safe.
- Preserve enough capacity for the host, SSH, Docker Engine, and Codex.
- Start only the services required for the current task. Use explicit service names and profiles.
- Start dependency groups incrementally. Verify health and resource usage after each group.
- Stop before starting more services if memory, swap, CPU, I/O, disk, or health becomes unsafe.
- Do not bypass a failed resource check by retrying the unrestricted command.
- Do not use unbounded parallel builds or unbounded service scaling.
- Do not remove volumes or broadly stop unrelated containers without explicit user authorization.
