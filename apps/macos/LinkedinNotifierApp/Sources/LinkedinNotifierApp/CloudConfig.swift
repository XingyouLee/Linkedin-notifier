import Foundation
import NIOSSL
import PostgresNIO

enum CloudConfigError: LocalizedError, Equatable {
    case missingJobsDatabaseURL
    case invalidJobsDatabaseURL(String)

    var errorDescription: String? {
        switch self {
        case .missingJobsDatabaseURL:
            return "The packaged app is missing a JOBS_DB_URL value. Rebuild the app with LINKEDIN_NOTIFIER_JOBS_DB_URL or JOBS_DB_URL set."
        case let .invalidJobsDatabaseURL(value):
            return "The packaged app contains an invalid JOBS_DB_URL: \(value)"
        }
    }
}

struct CloudConfig: Sendable {
    enum TLSMode: Sendable, Equatable {
        case disable
        case prefer
        case require
    }

    struct DatabaseEndpoint: Sendable, Equatable {
        let host: String
        let port: Int
        let username: String
        let password: String?
        let database: String
        let tlsMode: TLSMode

        static func parse(urlString: String) throws -> DatabaseEndpoint {
            guard let components = URLComponents(string: urlString),
                  let scheme = components.scheme?.lowercased(),
                  scheme == "postgres" || scheme == "postgresql",
                  let host = components.host,
                  !host.isEmpty,
                  let username = components.user?.removingPercentEncoding,
                  !username.isEmpty
            else {
                throw CloudConfigError.invalidJobsDatabaseURL(urlString)
            }

            let password = components.password?.removingPercentEncoding
            let databasePath = components.path.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
            let database = databasePath.isEmpty ? username : databasePath
            let port = components.port ?? 5432

            let queryItems = components.queryItems ?? []
            let sslMode = queryItems.first(where: { $0.name.lowercased() == "sslmode" })?.value?.lowercased()

            let tlsMode: TLSMode
            switch sslMode {
            case "disable":
                tlsMode = .disable
            case "require", "verify-ca", "verify-full":
                tlsMode = .require
            case "prefer", "allow":
                tlsMode = .prefer
            default:
                tlsMode = ["localhost", "127.0.0.1"].contains(host.lowercased()) ? .disable : .prefer
            }

            return DatabaseEndpoint(
                host: host,
                port: port,
                username: username,
                password: password,
                database: database,
                tlsMode: tlsMode
            )
        }

        var redactedDescription: String {
            "\(username)@\(host):\(port)/\(database)"
        }
    }

    let jobsDatabaseURL: String
    let airflowWebURL: URL?
    let databaseEndpoint: DatabaseEndpoint

    static func load(
        bundle: Bundle = .main,
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) throws -> CloudConfig {
        let jobsDatabaseURL = (
            bundle.object(forInfoDictionaryKey: CloudBuildConfig.jobsDatabaseURLInfoKey) as? String
        )?.trimmingCharacters(in: .whitespacesAndNewlines)
            ?? environment[CloudBuildConfig.jobsDatabaseURLEnvironmentKey]?.trimmingCharacters(in: .whitespacesAndNewlines)

        guard let jobsDatabaseURL, !jobsDatabaseURL.isEmpty else {
            throw CloudConfigError.missingJobsDatabaseURL
        }

        let airflowWebURLString = (
            bundle.object(forInfoDictionaryKey: CloudBuildConfig.airflowWebURLInfoKey) as? String
        )?.trimmingCharacters(in: .whitespacesAndNewlines)
            ?? environment[CloudBuildConfig.airflowWebURLEnvironmentKey]?.trimmingCharacters(in: .whitespacesAndNewlines)

        return try CloudConfig(
            jobsDatabaseURL: jobsDatabaseURL,
            airflowWebURL: airflowWebURLString.flatMap { $0.isEmpty ? nil : URL(string: $0) },
            databaseEndpoint: .parse(urlString: jobsDatabaseURL)
        )
    }

    var isAirflowConfigured: Bool {
        airflowWebURL != nil
    }

    var airflowWebURLString: String? {
        airflowWebURL?.absoluteString
    }

    var redactedDatabaseDescription: String {
        databaseEndpoint.redactedDescription
    }

    var postgresConnectionConfiguration: PostgresConnection.Configuration {
        let tlsContext = try? NIOSSLContext(configuration: .makeClientConfiguration())
        let tls: PostgresConnection.Configuration.TLS

        switch databaseEndpoint.tlsMode {
        case .disable:
            tls = .disable
        case .prefer:
            if let tlsContext {
                tls = .prefer(tlsContext)
            } else {
                tls = .disable
            }
        case .require:
            if let tlsContext {
                tls = .require(tlsContext)
            } else {
                tls = .disable
            }
        }

        var configuration = PostgresConnection.Configuration(
            host: databaseEndpoint.host,
            port: databaseEndpoint.port,
            username: databaseEndpoint.username,
            password: databaseEndpoint.password,
            database: databaseEndpoint.database,
            tls: tls
        )
        configuration.options.connectTimeout = .seconds(15)
        return configuration
    }

    func airflowHomeURL() -> URL? {
        airflowWebURL
    }

    func airflowDagURL(dagID: String) -> URL? {
        airflowWebURL?
            .appendingPathComponent("dags")
            .appendingPathComponent(dagID)
            .appendingPathComponent("grid")
    }

    func airflowDagRunURL(dagID: String, runID: String) -> URL? {
        airflowWebURL?
            .appendingPathComponent("dags")
            .appendingPathComponent(dagID)
            .appendingPathComponent("runs")
            .appendingPathComponent(runID)
    }
}
