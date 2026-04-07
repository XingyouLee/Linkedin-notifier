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
    dependencies: [
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.21.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.29.0"),
    ],
    targets: [
        .executableTarget(
            name: "LinkedinNotifierApp",
            dependencies: [
                .product(name: "PostgresNIO", package: "postgres-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
            ],
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
