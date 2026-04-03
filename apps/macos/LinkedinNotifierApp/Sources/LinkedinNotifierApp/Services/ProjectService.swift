import Foundation

final class ProjectService: Sendable {
    private let runner: CommandRunner

    init(runner: CommandRunner = CommandRunner()) {
        self.runner = runner
    }

    func refreshEnvironment(projectPath: String) async -> EnvironmentSnapshot {
        let status = await refreshEnvironmentStatus(projectPath: projectPath)
        guard status.astroRunning else {
            return status
        }

        let summaryResult = await refreshEnvironmentSummary(projectPath: projectPath)
        return status.withSummary(summaryResult.summary, lastError: summaryResult.lastError)
    }

    func refreshEnvironmentStatus(projectPath: String) async -> EnvironmentSnapshot {
        let dockerRunning = await isDockerRunning()
        guard dockerRunning else {
            return .empty
        }

        let containers = await projectContainers(projectPath: projectPath)
        let scheduler = await containerName(projectPath: projectPath, service: "scheduler")
        let apiServer = await containerName(projectPath: projectPath, service: "api-server")
        let postgres = await containerName(projectPath: projectPath, service: "postgres")
        let astroRunning = scheduler != nil

        return EnvironmentSnapshot(
            dockerRunning: dockerRunning,
            astroRunning: astroRunning,
            schedulerContainer: scheduler,
            apiServerContainer: apiServer,
            postgresContainer: postgres,
            containers: containers,
            summary: nil,
            lastError: nil
        )
    }

    func refreshEnvironmentSummary(projectPath: String) async -> (summary: SummaryPayload?, lastError: String?) {
        guard await containerName(projectPath: projectPath, service: "scheduler") != nil else {
            return (nil, nil)
        }

        do {
            let summary = try await loadSummary(projectPath: projectPath)
            return (summary, nil)
        } catch {
            return (nil, error.localizedDescription)
        }
    }

    func startAstro(projectPath: String) async throws -> String {
        let result = try await runner.run(
            arguments: ["astro", "dev", "start", "--no-browser"],
            currentDirectory: projectPath,
            timeout: 180
        )
        return result.combinedOutput
    }

    func triggerDag(projectPath: String, dagID: String) async throws -> DagRunRecord? {
        let scheduler = try await requireContainer(projectPath: projectPath, service: "scheduler")
        let result = try await runner.run(
            arguments: ["docker", "exec", scheduler, "airflow", "dags", "trigger", dagID, "-o", "json"],
            currentDirectory: projectPath,
            timeout: 30
        )
        if let run = try? runner.decodeJSON(DagRunRecord.self, from: result.combinedOutput) {
            return run
        }
        let runs = try runner.decodeJSON([DagRunRecord].self, from: result.combinedOutput)
        return runs.first
    }

    func listDagRuns(projectPath: String, dagID: String) async throws -> [DagRunRecord] {
        let scheduler = try await requireContainer(projectPath: projectPath, service: "scheduler")
        let result = try await runner.run(
            arguments: ["docker", "exec", scheduler, "airflow", "dags", "list-runs", dagID, "-o", "json"],
            currentDirectory: projectPath,
            timeout: 30
        )
        return try runner.decodeJSON([DagRunRecord].self, from: result.combinedOutput)
    }

    func loadSummary(projectPath: String) async throws -> SummaryPayload {
        let scheduler = try await requireContainer(projectPath: projectPath, service: "scheduler")
        let result = try await runner.run(
            arguments: [
                "docker", "exec", scheduler, "python",
                "/usr/local/airflow/dags/frontend_data.py", "summary",
            ],
            currentDirectory: projectPath,
            timeout: 20
        )
        return try runner.decodeJSON(SummaryPayload.self, from: result.combinedOutput)
    }

    func loadJobs(projectPath: String, searchText: String, limit: Int = 50) async throws -> [JobListItem] {
        let scheduler = try await requireContainer(projectPath: projectPath, service: "scheduler")
        var arguments = [
            "docker", "exec", scheduler, "python",
            "/usr/local/airflow/dags/frontend_data.py", "jobs", "list",
            "--limit", String(limit),
        ]
        let trimmedSearch = searchText.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmedSearch.isEmpty {
            arguments.append(contentsOf: ["--query", trimmedSearch])
        }
        let result = try await runner.run(
            arguments: arguments,
            currentDirectory: projectPath,
            timeout: 20
        )
        return try runner.decodeJSON([JobListItem].self, from: result.combinedOutput)
    }

    func loadJobDetail(projectPath: String, jobID: String) async throws -> JobDetailResponse {
        let scheduler = try await requireContainer(projectPath: projectPath, service: "scheduler")
        let result = try await runner.run(
            arguments: [
                "docker", "exec", scheduler, "python",
                "/usr/local/airflow/dags/frontend_data.py", "jobs", "get", jobID,
            ],
            currentDirectory: projectPath,
            timeout: 20
        )
        return try runner.decodeJSON(JobDetailResponse.self, from: result.combinedOutput)
    }

    func loadProfileDashboards(projectPath: String) async throws -> ProfileDashboardResponse {
        let scheduler = try await requireContainer(projectPath: projectPath, service: "scheduler")
        let result = try await runner.run(
            arguments: [
                "docker", "exec", scheduler, "python",
                "/usr/local/airflow/dags/frontend_data.py", "profiles", "dashboard",
            ],
            currentDirectory: projectPath,
            timeout: 20
        )
        return try runner.decodeJSON(ProfileDashboardResponse.self, from: result.combinedOutput)
    }

    func airflowDagRunURL(projectPath: String, dagID: String, runID: String) async -> URL? {
        let baseURL = await airflowBaseURL(projectPath: projectPath)
            ?? URL(string: "http://127.0.0.1:8080")
        guard let baseURL else {
            return nil
        }

        return baseURL
            .appendingPathComponent("dags")
            .appendingPathComponent(dagID)
            .appendingPathComponent("runs")
            .appendingPathComponent(runID)
    }

    private func isDockerRunning() async -> Bool {
        do {
            _ = try await runner.run(
                arguments: ["docker", "info", "--format", "{{.ServerVersion}}"],
                timeout: 10
            )
            return true
        } catch {
            return false
        }
    }

    private func projectContainers(projectPath: String) async -> [ContainerInfo] {
        do {
            let result = try await runner.run(
                arguments: [
                    "docker", "ps", "-a",
                    "--filter", "label=com.docker.compose.project.working_dir=\(projectPath)",
                    "--format", "{{.Names}}|{{.Status}}",
                ],
                currentDirectory: projectPath,
                timeout: 10
            )
            return result.standardOutput
                .split(separator: "\n")
                .compactMap { line in
                    let pieces = line.split(separator: "|", maxSplits: 1, omittingEmptySubsequences: false)
                    guard pieces.count == 2 else { return nil }
                    return ContainerInfo(name: String(pieces[0]), status: String(pieces[1]))
                }
        } catch {
            return []
        }
    }

    private func requireContainer(projectPath: String, service: String) async throws -> String {
        if let name = await containerName(projectPath: projectPath, service: service) {
            return name
        }
        throw CommandError.launchFailed("Could not find a running \(service) container for \(projectPath).")
    }

    private func airflowBaseURL(projectPath: String) async -> URL? {
        guard let apiServer = await containerName(projectPath: projectPath, service: "api-server") else {
            return nil
        }

        do {
            let result = try await runner.run(
                arguments: ["docker", "port", apiServer, "8080/tcp"],
                currentDirectory: projectPath,
                timeout: 10
            )
            return Self.airflowBaseURL(fromDockerPortOutput: result.standardOutput)
        } catch {
            return nil
        }
    }

    private func containerName(projectPath: String, service: String) async -> String? {
        do {
            let result = try await runner.run(
                arguments: [
                    "docker", "ps",
                    "--filter", "label=com.docker.compose.project.working_dir=\(projectPath)",
                    "--filter", "label=com.docker.compose.service=\(service)",
                    "--format", "{{.Names}}",
                ],
                currentDirectory: projectPath,
                timeout: 10
            )
            return result.standardOutput
                .split(separator: "\n")
                .map(String.init)
                .first(where: { !$0.isEmpty })
        } catch {
            return nil
        }
    }

    static func airflowBaseURL(fromDockerPortOutput output: String) -> URL? {
        let lines = output
            .split(separator: "\n")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }

        for line in lines {
            if let url = airflowBaseURL(fromDockerPortLine: line) {
                return url
            }
        }

        return nil
    }

    private static func airflowBaseURL(fromDockerPortLine line: String) -> URL? {
        if line.hasPrefix("[::]:") {
            let port = String(line.dropFirst(5))
            return URL(string: "http://127.0.0.1:\(port)")
        }

        let pieces = line.split(separator: ":")
        guard pieces.count >= 2, let port = pieces.last else {
            return nil
        }

        let host = String(pieces.dropLast().joined(separator: ":"))
        let normalizedHost = host == "0.0.0.0" ? "127.0.0.1" : host
        return URL(string: "http://\(normalizedHost):\(port)")
    }
}
