import Foundation

enum ProjectLocator {
    static let projectPathDefaultsKey = "LinkedinNotifierApp.projectPath"
    static let bundleProjectPathKey = "LinkedinNotifierProjectPath"

    static func defaultProjectPath(
        filePath: String = #filePath,
        userDefaults: UserDefaults = .standard,
        bundle: Bundle = .main
    ) -> String {
        resolvedDefaultProjectPath(
            savedProjectPath: userDefaults.string(forKey: projectPathDefaultsKey),
            bundleProjectPath: bundle.object(forInfoDictionaryKey: bundleProjectPathKey) as? String,
            filePath: filePath,
            homeDirectoryPath: FileManager.default.homeDirectoryForCurrentUser.path,
            fileExistsAtPath: { FileManager.default.fileExists(atPath: $0) }
        )
    }

    static func persistProjectPath(_ projectPath: String, userDefaults: UserDefaults = .standard) {
        let trimmedPath = projectPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedPath.isEmpty else { return }
        userDefaults.set(trimmedPath, forKey: projectPathDefaultsKey)
    }

    static func resolvedDefaultProjectPath(
        savedProjectPath: String?,
        bundleProjectPath: String?,
        filePath: String,
        homeDirectoryPath: String,
        fileExistsAtPath: (String) -> Bool
    ) -> String {
        if let savedProjectPath,
           pathContainsAstroConfig(savedProjectPath, fileExistsAtPath: fileExistsAtPath) {
            return savedProjectPath
        }

        if let bundleProjectPath,
           pathContainsAstroConfig(bundleProjectPath, fileExistsAtPath: fileExistsAtPath) {
            return bundleProjectPath
        }

        let sourceURL = URL(fileURLWithPath: filePath).deletingLastPathComponent()
        for candidate in sourceURL.ancestorChain() {
            let astroConfig = candidate.appendingPathComponent(".astro/config.yaml")
            if fileExistsAtPath(astroConfig.path) {
                return candidate.path
            }
        }

        return homeDirectoryPath
    }

    static func pathContainsAstroConfig(
        _ path: String,
        fileExistsAtPath: (String) -> Bool
    ) -> Bool {
        let astroConfigPath = URL(fileURLWithPath: path)
            .appendingPathComponent(".astro/config.yaml")
            .path
        return fileExistsAtPath(astroConfigPath)
    }
}

private extension URL {
    func ancestorChain() -> [URL] {
        var chain: [URL] = []
        var current = self
        while true {
            chain.append(current)
            let parent = current.deletingLastPathComponent()
            if parent.path == current.path {
                break
            }
            current = parent
        }
        return chain
    }
}
