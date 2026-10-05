import SwiftUI

private enum AppTab: Hashable {
    case overview
    case profiles
    case jobs
}

struct ContentView: View {
    @State private var selectedTab: AppTab = .overview

    var body: some View {
        TabView(selection: $selectedTab) {
            OverviewTabView(isVisible: selectedTab == .overview)
                .tabItem {
                    Label("Overview", systemImage: "gauge.with.needle")
                }
                .tag(AppTab.overview)

            ProfilesTabView(isVisible: selectedTab == .profiles)
                .tabItem {
                    Label("Profiles", systemImage: "person.text.rectangle")
                }
                .tag(AppTab.profiles)

            JobsTabView()
                .tabItem {
                    Label("Jobs", systemImage: "list.bullet.rectangle")
                }
                .tag(AppTab.jobs)
        }
        .padding(.top, 10)
        .toolbar {
            ToolbarItemGroup(placement: .primaryAction) {
                Button {
                    if let url = URL(string: "file://\(CloudConfig.userConfigFileURL.path)") {
                        NSWorkspace.shared.selectFile(nil, inFileViewerRootedAtPath: url.deletingLastPathComponent().path)
                    }
                } label: {
                    Label("Reveal Config", systemImage: "gearshape")
                }
                .help("Reveal database configuration file in Finder")
            }
        }
    }
}
