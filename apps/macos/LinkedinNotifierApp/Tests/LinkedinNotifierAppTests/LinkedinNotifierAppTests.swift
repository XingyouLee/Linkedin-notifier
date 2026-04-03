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
func summaryPayloadDecodesGlobalProgressFields() throws {
    let data = Data(
        """
        {
          "jd_pending": 3,
          "jd_processing": 2,
          "jd_failed": 1,
          "jd_done": 4,
          "jd_global_total": 10,
          "fit_pending": 5,
          "fit_processing": 2,
          "fit_failed": 1,
          "fit_done": 6,
          "fit_terminal_done": 7,
          "fit_global_total": 15
        }
        """.utf8
    )

    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase

    let summary = try decoder.decode(SummaryPayload.self, from: data)

    #expect(summary.jdPending == 3)
    #expect(summary.jdProcessing == 2)
    #expect(summary.jdFailed == 1)
    #expect(summary.jdDone == 4)
    #expect(summary.jdGlobalTotal == 10)
    #expect(summary.fitPending == 5)
    #expect(summary.fitProcessing == 2)
    #expect(summary.fitFailed == 1)
    #expect(summary.fitDone == 6)
    #expect(summary.fitTerminalDone == 7)
    #expect(summary.fitGlobalTotal == 15)
}

@Test
func summaryPayloadComputesGlobalQueueProgressFromConsistentMetrics() {
    let summary = SummaryPayload(
        totalJobs: nil,
        totalProfileJobs: nil,
        jdPending: 11,
        jdProcessing: 7,
        jdFailed: 2,
        jdDone: 70,
        jdGlobalTotal: 20,
        fitPending: 20,
        fitProcessing: 53,
        fitFailed: 4,
        fitDone: 59,
        fitTerminalDone: 59,
        fitGlobalTotal: 77,
        notifyFailed: 3,
        latestBatchId: 123,
        latestBatchJobsTotal: 70,
        latestBatchJdDone: 70,
        latestBatchJdProcessing: 0,
        latestBatchJdPending: 0,
        latestBatchJdFailed: 1,
        latestBatchFitTotal: 143,
        latestBatchFitDone: 59,
        latestBatchFitProcessing: 53,
        latestBatchFitPending: 20,
        latestBatchFitFailed: 0,
        latestBatchScanProgress: 100
    )

    #expect(summary.globalJdCompletedCount == 72)
    #expect(summary.globalJdTotalCount == 90)
    #expect(summary.globalFitCompletedCount == 66)
    #expect(summary.globalFitTotalCount == 139)
}

@Test
func profileDashboardResponseDecodesDashboardPayload() throws {
    let data = Data(
        """
        {
          "profiles": [
            {
              "profile_id": 2,
              "profile_key": "Xingyou Li",
              "display_name": "Xingyou Li",
              "is_active": true,
              "model_name": "gpt-5.4",
              "discord_channel_id": "123456",
              "has_discord_webhook": true,
              "total_jobs": 100,
              "notified_jobs": 12,
              "fit_pending": 3,
              "fit_processing": 1,
              "fit_done": 60,
              "fit_notified": 10,
              "fit_failed": 2,
              "notify_failed": 1,
              "scored_jobs": 73,
              "avg_fit_score": 31.4,
              "max_fit_score": 88,
              "last_seen_at": "2026-03-31 19:14:09",
              "last_notified_at": "2026-03-31 12:00:00",
              "score_buckets": [
                {
                  "bucket_index": 0,
                  "label": "0-9",
                  "lower_bound": 0,
                  "upper_bound": 9,
                  "count": 4
                },
                {
                  "bucket_index": 1,
                  "label": "10-19",
                  "lower_bound": 10,
                  "upper_bound": 19,
                  "count": 6
                }
              ],
              "decision_breakdown": [
                { "decision": "Strong Fit", "count": 1 },
                { "decision": "Moderate Fit", "count": 4 },
                { "decision": "Weak Fit", "count": 12 },
                { "decision": "Not Recommended", "count": 40 }
              ],
              "top_terms": [
                { "term": "Data Engineer", "count": 44 },
                { "term": "Python Developer", "count": 10 }
              ]
            }
          ]
        }
        """.utf8
    )

    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase

    let response = try decoder.decode(ProfileDashboardResponse.self, from: data)
    let profile = try #require(response.profiles.first)

    #expect(profile.profileId == 2)
    #expect(profile.name == "Xingyou Li")
    #expect(profile.totalJobsValue == 100)
    #expect(profile.notifiedJobsValue == 12)
    #expect(profile.scoredJobsValue == 73)
    #expect(profile.terminalCompletedJobs == 70)
    #expect(profile.totalFailures == 3)
    #expect(profile.strongOrModerateCount == 5)
    #expect(profile.scoreBuckets.count == 2)
    #expect(profile.topTerms.count == 2)
}
