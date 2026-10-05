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
                } else if context.needsDatabaseURL {
                    DatabaseURLSetupView(context: context)
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

private struct DatabaseURLSetupView: View {
    @ObservedObject var context: AppContext
    @State private var errorMessage: String?

    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            Text("Database Configuration")
                .font(.largeTitle.bold())

            Text("Enter your database connection URL to get started.")
                .foregroundStyle(.secondary)

            TextField("postgresql://user:password@host:5432/dbname", text: $context.pendingDatabaseURL)
                .textFieldStyle(.roundedBorder)
                .font(.system(.body, design: .monospaced))

            if let errorMessage {
                Text(errorMessage)
                    .foregroundStyle(.red)
            }

            HStack {
                Spacer()
                Button("Connect") {
                    errorMessage = nil
                    let input = context.pendingDatabaseURL.trimmingCharacters(in: .whitespacesAndNewlines)
                    guard !input.isEmpty else {
                        errorMessage = "Please enter a database URL."
                        return
                    }
                    do {
                        // Validate by parsing
                        _ = try CloudConfig.DatabaseEndpoint.parse(urlString: input)
                        try CloudConfig.saveUserConfigFile(jobsDatabaseURL: input)
                        let alert = NSAlert()
                        alert.messageText = "Configuration Saved"
                        alert.informativeText = "Your database URL has been saved. The app will restart now."
                        alert.runModal()
                        let appURL = URL(fileURLWithPath: Bundle.main.resourcePath!)
                            .deletingLastPathComponent().deletingLastPathComponent()
                        do {
                            try Process.run(URL(fileURLWithPath: "/usr/bin/open"), arguments: [appURL.path])
                        } catch {
                            // Restart failed — log but still quit; user can relaunch manually
                            print("Failed to restart app: \(error)")
                        }
                        NSApplication.shared.terminate(nil)
                    } catch {
                        errorMessage = error.localizedDescription
                    }
                }
                .disabled(context.pendingDatabaseURL.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                .buttonStyle(.borderedProminent)
            }

            Spacer()
        }
        .padding(32)
        .frame(maxWidth: 600, maxHeight: .infinity, alignment: .topLeading)
    }
}
