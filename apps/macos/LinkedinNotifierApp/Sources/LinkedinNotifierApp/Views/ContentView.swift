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
    }
}
