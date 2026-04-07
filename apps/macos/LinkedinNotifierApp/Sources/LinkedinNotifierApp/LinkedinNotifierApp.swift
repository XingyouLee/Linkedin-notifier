import AppKit
import SwiftUI

final class LinkedinNotifierAppDelegate: NSObject, NSApplicationDelegate {
    func applicationDidFinishLaunching(_ notification: Notification) {
        let bundledIconName = (Bundle.main.object(forInfoDictionaryKey: "CFBundleIconFile") as? String)?
            .trimmingCharacters(in: .whitespacesAndNewlines)

        if (bundledIconName ?? "").isEmpty,
           let iconURL = Bundle.module.url(forResource: "AppIcon", withExtension: "png"),
           let iconImage = NSImage(contentsOf: iconURL) {
            NSApp.applicationIconImage = iconImage
        }
        NSApp.activate(ignoringOtherApps: true)
    }
}

@main
struct LinkedinNotifierApp: App {
    @NSApplicationDelegateAdaptor(LinkedinNotifierAppDelegate.self)
    private var appDelegate
    @StateObject private var context = AppContext()

    var body: some Scene {
        WindowGroup {
            Group {
                if context.isReady {
                    ContentView()
                        .environmentObject(context)
                } else {
                    StartupErrorView(message: context.startupError ?? "Missing cloud configuration.")
                }
            }
            .frame(minWidth: 1200, minHeight: 780)
        }
        .windowResizability(.contentMinSize)
    }
}

private struct StartupErrorView: View {
    let message: String

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Cloud Client Configuration Error")
                .font(.largeTitle.bold())
            Text("This build could not load its packaged cloud settings.")
                .foregroundStyle(.secondary)
            Text(message)
                .textSelection(.enabled)
                .padding(16)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(.quaternary.opacity(0.35), in: RoundedRectangle(cornerRadius: 14))
        }
        .padding(32)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}
