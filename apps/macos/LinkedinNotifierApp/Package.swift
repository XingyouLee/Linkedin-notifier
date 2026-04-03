// swift-tools-version: 6.3

import PackageDescription

let package = Package(
    name: "LinkedinNotifierApp",
    platforms: [
        .macOS(.v14),
    ],
    products: [
        .executable(
            name: "LinkedinNotifierApp",
            targets: ["LinkedinNotifierApp"]
        ),
    ],
    targets: [
        .executableTarget(
            name: "LinkedinNotifierApp",
            resources: [
                .process("Resources"),
            ]
        ),
        .testTarget(
            name: "LinkedinNotifierAppTests",
            dependencies: ["LinkedinNotifierApp"]
        ),
    ],
    swiftLanguageModes: [.v6]
)
