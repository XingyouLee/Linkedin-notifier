import Foundation

enum CloudBuildConfig {
    static let jobsDatabaseURLInfoKey = "LinkedinNotifierJobsDatabaseURL"
    static let airflowWebURLInfoKey = "LinkedinNotifierAirflowWebURL"

    static let jobsDatabaseURLEnvironmentKey = "LINKEDIN_NOTIFIER_JOBS_DB_URL"
    static let airflowWebURLEnvironmentKey = "LINKEDIN_NOTIFIER_AIRFLOW_WEB_URL"
}
