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
            ContentView()
                .environmentObject(context)
                .frame(minWidth: 1200, minHeight: 780)
        }
        .windowResizability(.contentMinSize)
    }
}
