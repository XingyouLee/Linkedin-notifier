import Foundation

@MainActor
final class AppContext: ObservableObject {
    @Published var projectPath: String {
        didSet {
            ProjectLocator.persistProjectPath(projectPath)
        }
    }

    let projectService: ProjectService

    init(
        projectPath: String = ProjectLocator.defaultProjectPath(),
        projectService: ProjectService? = nil
    ) {
        self.projectPath = projectPath
        self.projectService = projectService ?? ProjectService()
        ProjectLocator.persistProjectPath(projectPath)
    }
}
