import Foundation

@MainActor
final class AppContext: ObservableObject {
    enum State {
        case needsDatabaseURL
        case ready(ProjectService)
        case failed(String)
    }

    let state: State

    @Published var pendingDatabaseURL: String = ""

    init(
        projectService: ProjectService? = nil
    ) {
        if let projectService {
            self.state = .ready(projectService)
            return
        }

        do {
            self.state = .ready(try ProjectService.loadDefault())
        } catch let error as CloudConfigError where error == .missingJobsDatabaseURL {
            self.state = .needsDatabaseURL
        } catch {
            self.state = .failed(error.localizedDescription)
        }
    }

    var isReady: Bool {
        if case .ready = state {
            return true
        }
        return false
    }

    var needsDatabaseURL: Bool {
        if case .needsDatabaseURL = state {
            return true
        }
        return false
    }

    var startupError: String? {
        if case let .failed(message) = state {
            return message
        }
        return nil
    }

    var projectService: ProjectService {
        guard case let .ready(projectService) = state else {
            preconditionFailure("ProjectService is unavailable because cloud config failed to load.")
        }
        return projectService
    }

    var cloudConfig: CloudConfig {
        projectService.cloudConfig
    }

    static func deleteSavedConfig() throws {
        let url = CloudConfig.userConfigFileURL
        if FileManager.default.fileExists(atPath: url.path) {
            try FileManager.default.removeItem(at: url)
        }
    }
}
