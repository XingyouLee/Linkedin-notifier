import Foundation

enum LocalProfilesStoreError: LocalizedError {
    case missingProfilesJSON

    var errorDescription: String? {
        switch self {
        case .missingProfilesJSON:
            return "The packaged app could not find its local profiles.json resource."
        }
    }
}

struct LocalProfilesStore {
    private let decoder: JSONDecoder

    init() {
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        self.decoder = decoder
    }

    func loadProfiles() throws -> [LocalProfileSummary] {
        let data = try Data(contentsOf: profilesURL())
        return try decoder.decode([LocalProfileSummary].self, from: data)
    }

    private func profilesURL() throws -> URL {
        if let bundledURL = Bundle.main.url(forResource: "profiles", withExtension: "json") {
            return bundledURL
        }

        if let moduleURL = Bundle.module.url(forResource: "profiles", withExtension: "json") {
            return moduleURL
        }

        let repoURL = URL(fileURLWithPath: "/Users/levi/Linkedin-notifier/include/user_info/profiles.json")
        if FileManager.default.fileExists(atPath: repoURL.path) {
            return repoURL
        }

        throw LocalProfilesStoreError.missingProfilesJSON
    }
}
