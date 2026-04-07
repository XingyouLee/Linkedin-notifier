import Foundation

@MainActor
final class AppContext: ObservableObject {
    enum State {
        case ready(ProjectService)
        case failed(String)
    }

    let state: State

    init(
        projectService: ProjectService? = nil
    ) {
        if let projectService {
            self.state = .ready(projectService)
            return
        }

        do {
            self.state = .ready(try ProjectService.loadDefault())
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
}
