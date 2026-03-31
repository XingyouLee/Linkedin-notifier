import Foundation
import Testing

@testable import LinkedinNotifierApp

@Test
func commandRunnerReadsLargeOutputWithoutDeadlocking() async throws {
    let runner = CommandRunner()

    let result = try await runner.run(
        executable: "/usr/bin/python3",
        arguments: ["-c", "import sys; sys.stdout.write('x' * 70000)"],
        timeout: 10
    )

    #expect(result.exitCode == 0)
    #expect(result.standardOutput.count == 70000)
    #expect(result.standardError.isEmpty)
}

@Test
func commandRunnerTimesOutHungProcess() async {
    let runner = CommandRunner()

    do {
        _ = try await runner.run(
            executable: "/bin/sleep",
            arguments: ["2"],
            timeout: 0.1
        )
        Issue.record("Expected the sleep command to time out.")
    } catch let error as CommandError {
        switch error {
        case .timedOut:
            break
        default:
            Issue.record("Expected a timeout error, got \(error).")
        }
    } catch {
        Issue.record("Expected a CommandError timeout, got \(error).")
    }
}

@Test
func decodeJSONFindsPayloadInsideNoisyAirflowOutput() throws {
    let runner = CommandRunner()
    let output = """
    2026-03-31T06:32:35.438488Z [warning  ] Astro managed secrets backend is disabled [astronomer.runtime.plugin.AstronomerRuntimePlugin] loc=plugin.py:125
    [{"dag_id":"linkedin_notifier","run_id":"scheduled__2026-03-31T00:00:00+00:00","state":"success","run_after":"2026-03-31T00:00:00+00:00"}]
    /usr/local/lib/python3.12/site-packages/airflow/configuration.py:879 DeprecationWarning: The secret_key option in [webserver] has been moved to the secret_key option in [api]
    """

    let runs = try runner.decodeJSON([DagRunRecord].self, from: output)

    #expect(runs.count == 1)
    #expect(runs.first?.dagId == "linkedin_notifier")
    #expect(runs.first?.runId == "scheduled__2026-03-31T00:00:00+00:00")
    #expect(runs.first?.state == "success")
}

@Test
func airflowBaseURLParsesDockerPortOutput() throws {
    let url = ProjectService.airflowBaseURL(
        fromDockerPortOutput: """
        127.0.0.1:8080
        [::]:8080
        """
    )

    #expect(url?.absoluteString == "http://127.0.0.1:8080")
}

@Test
func projectLocatorPrefersSavedProjectPathWhenValid() {
    let path = ProjectLocator.resolvedDefaultProjectPath(
        savedProjectPath: "/tmp/saved-project",
        bundleProjectPath: "/tmp/bundle-project",
        filePath: "/tmp/source/LinkedinNotifierApp.swift",
        homeDirectoryPath: "/Users/levi",
        fileExistsAtPath: { filePath in
            filePath == "/tmp/saved-project/.astro/config.yaml"
        }
    )

    #expect(path == "/tmp/saved-project")
}

@Test
func projectLocatorFallsBackToBundledProjectPathWhenSavedPathIsInvalid() {
    let path = ProjectLocator.resolvedDefaultProjectPath(
        savedProjectPath: "/tmp/missing-project",
        bundleProjectPath: "/tmp/bundle-project",
        filePath: "/tmp/source/LinkedinNotifierApp.swift",
        homeDirectoryPath: "/Users/levi",
        fileExistsAtPath: { filePath in
            filePath == "/tmp/bundle-project/.astro/config.yaml"
        }
    )

    #expect(path == "/tmp/bundle-project")
}

@Test
func dagRunRecordDecodesListRunsPayload() throws {
    let data = Data(
        """
        {
          "dag_id": "linkedin_notifier",
          "run_id": "manual__2026-03-30T20:00:00+00:00",
          "state": "success"
        }
        """.utf8
    )

    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase

    let run = try decoder.decode(DagRunRecord.self, from: data)

    #expect(run.dagId == "linkedin_notifier")
    #expect(run.runId == "manual__2026-03-30T20:00:00+00:00")
    #expect(run.state == "success")
}

@Test
func dagRunRecordDecodesTriggerPayload() throws {
    let data = Data(
        """
        {
          "dag_id": "linkedin_notifier",
          "dag_run_id": "manual__2026-03-30T20:00:00+00:00",
          "state": "queued"
        }
        """.utf8
    )

    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase

    let run = try decoder.decode(DagRunRecord.self, from: data)

    #expect(run.dagId == "linkedin_notifier")
    #expect(run.runId == "manual__2026-03-30T20:00:00+00:00")
    #expect(run.state == "queued")
}

@Test
func commandRunnerDecodeJSONParsesSingleTriggerObject() throws {
    let runner = CommandRunner()
    let output = """
    {"dag_id":"linkedin_fitting_notifier","dag_run_id":"manual__2026-03-31T10:00:00+00:00","state":"queued"}
    """

    let run = try runner.decodeJSON(DagRunRecord.self, from: output)

    #expect(run.dagId == "linkedin_fitting_notifier")
    #expect(run.runId == "manual__2026-03-31T10:00:00+00:00")
    #expect(run.state == "queued")
}
