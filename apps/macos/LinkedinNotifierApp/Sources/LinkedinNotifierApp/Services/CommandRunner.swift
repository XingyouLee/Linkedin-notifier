import Foundation

struct CommandResult: Hashable, Sendable {
    let standardOutput: String
    let standardError: String
    let exitCode: Int32

    var combinedOutput: String {
        [standardOutput, standardError]
            .filter { !$0.isEmpty }
            .joined(separator: "\n")
    }
}

enum CommandError: LocalizedError, Sendable {
    case launchFailed(String)
    case nonZeroExit(code: Int32, output: String)
    case malformedJSON(String)
    case timedOut(seconds: TimeInterval, output: String)

    var errorDescription: String? {
        switch self {
        case let .launchFailed(message):
            return message
        case let .nonZeroExit(code, output):
            return "Command failed with exit code \(code): \(output)"
        case let .malformedJSON(output):
            return "Could not decode JSON from command output: \(output)"
        case let .timedOut(seconds, output):
            let details = output.isEmpty ? "" : " Output: \(output)"
            return "Command timed out after \(Int(seconds))s.\(details)"
        }
    }
}

actor CommandRunner {
    func run(
        executable: String = "/usr/bin/env",
        arguments: [String],
        currentDirectory: String? = nil,
        timeout: TimeInterval = 30
    ) async throws -> CommandResult {
        try await Task.detached(priority: .userInitiated) {
            let process = Process()
            let stdout = Pipe()
            let stderr = Pipe()
            let stdoutBuffer = OutputBuffer()
            let stderrBuffer = OutputBuffer()
            let completionSignal = DispatchSemaphore(value: 0)

            process.executableURL = URL(fileURLWithPath: executable)
            process.arguments = arguments
            process.standardOutput = stdout
            process.standardError = stderr
            process.environment = self.buildEnvironment()
            if let currentDirectory {
                process.currentDirectoryURL = URL(fileURLWithPath: currentDirectory)
            }

            if executable == "/usr/bin/env", let commandName = arguments.first {
                if let resolvedExecutable = self.resolveCommand(named: commandName) {
                    process.executableURL = URL(fileURLWithPath: resolvedExecutable)
                    process.arguments = Array(arguments.dropFirst())
                }
            }

            stdout.fileHandleForReading.readabilityHandler = { handle in
                let data = handle.availableData
                guard !data.isEmpty else {
                    handle.readabilityHandler = nil
                    return
                }
                stdoutBuffer.append(data)
            }

            stderr.fileHandleForReading.readabilityHandler = { handle in
                let data = handle.availableData
                guard !data.isEmpty else {
                    handle.readabilityHandler = nil
                    return
                }
                stderrBuffer.append(data)
            }

            process.terminationHandler = { _ in
                completionSignal.signal()
            }

            do {
                try process.run()
            } catch {
                throw CommandError.launchFailed(error.localizedDescription)
            }

            if self.waitForExitSignal(completionSignal, timeout: timeout) == .timedOut {
                if process.isRunning {
                    process.interrupt()
                    process.terminate()
                    _ = self.waitForExitSignal(completionSignal, timeout: 2)
                }

                let timedOutResult = self.buildResult(
                    stdoutPipe: stdout,
                    stderrPipe: stderr,
                    stdoutBuffer: stdoutBuffer,
                    stderrBuffer: stderrBuffer,
                    exitCode: process.terminationStatus
                )
                throw CommandError.timedOut(
                    seconds: timeout,
                    output: timedOutResult.combinedOutput
                )
            }

            let result = self.buildResult(
                stdoutPipe: stdout,
                stderrPipe: stderr,
                stdoutBuffer: stdoutBuffer,
                stderrBuffer: stderrBuffer,
                exitCode: process.terminationStatus
            )

            if result.exitCode != 0 {
                throw CommandError.nonZeroExit(
                    code: result.exitCode,
                    output: result.combinedOutput
                )
            }

            return result
        }.value
    }

    nonisolated func decodeJSON<T: Decodable>(_ type: T.Type, from output: String) throws -> T {
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase

        for candidate in Self.jsonCandidates(from: output) {
            guard let data = candidate.data(using: .utf8) else {
                continue
            }
            if let decoded = try? decoder.decode(type, from: data) {
                return decoded
            }
        }

        throw CommandError.malformedJSON(output)
    }

    static func extractJSONPayload(from output: String) -> String? {
        let characters = Array(output)

        for startOffset in characters.indices where characters[startOffset] == "[" || characters[startOffset] == "{" {
            guard let endOffset = findBalancedJSONEnd(in: characters, startOffset: startOffset) else {
                continue
            }

            let candidate = String(characters[startOffset...endOffset])
            guard let data = candidate.data(using: .utf8) else {
                continue
            }

            if (try? JSONSerialization.jsonObject(with: data)) != nil {
                return candidate
            }
        }

        return nil
    }

    private static func findBalancedJSONEnd(
        in characters: [Character],
        startOffset: Int
    ) -> Int? {
        guard startOffset < characters.count else { return nil }

        var stack: [Character] = [characters[startOffset]]
        var isInsideString = false
        var isEscaped = false

        for offset in (startOffset + 1)..<characters.count {
            let character = characters[offset]

            if isInsideString {
                if isEscaped {
                    isEscaped = false
                    continue
                }
                if character == "\\" {
                    isEscaped = true
                } else if character == "\"" {
                    isInsideString = false
                }
                continue
            }

            if character == "\"" {
                isInsideString = true
                continue
            }

            switch character {
            case "{", "[":
                stack.append(character)
            case "}":
                guard stack.last == "{" else { return nil }
                stack.removeLast()
            case "]":
                guard stack.last == "[" else { return nil }
                stack.removeLast()
            default:
                break
            }

            if stack.isEmpty {
                return offset
            }
        }

        return nil
    }

    static func jsonCandidates(from output: String) -> [String] {
        var candidates: [String] = []
        var seen = Set<String>()

        func appendCandidate(_ candidate: String?) {
            let trimmed = candidate?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            guard !trimmed.isEmpty else { return }
            guard seen.insert(trimmed).inserted else { return }
            candidates.append(trimmed)
        }

        appendCandidate(output)

        for line in output.split(separator: "\n", omittingEmptySubsequences: false) {
            let trimmedLine = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard trimmedLine.first == "[" || trimmedLine.first == "{" else {
                continue
            }
            appendCandidate(String(trimmedLine))
        }

        appendCandidate(extractJSONPayload(from: output))
        return candidates
    }

    private nonisolated func buildResult(
        stdoutPipe: Pipe,
        stderrPipe: Pipe,
        stdoutBuffer: OutputBuffer,
        stderrBuffer: OutputBuffer,
        exitCode: Int32
    ) -> CommandResult {
        stdoutPipe.fileHandleForReading.readabilityHandler = nil
        stderrPipe.fileHandleForReading.readabilityHandler = nil

        stdoutBuffer.append(stdoutPipe.fileHandleForReading.readDataToEndOfFile())
        stderrBuffer.append(stderrPipe.fileHandleForReading.readDataToEndOfFile())

        return CommandResult(
            standardOutput: String(decoding: stdoutBuffer.snapshot(), as: UTF8.self)
                .trimmingCharacters(in: .whitespacesAndNewlines),
            standardError: String(decoding: stderrBuffer.snapshot(), as: UTF8.self)
                .trimmingCharacters(in: .whitespacesAndNewlines),
            exitCode: exitCode
        )
    }

    private nonisolated func waitForExitSignal(
        _ signal: DispatchSemaphore,
        timeout: TimeInterval
    ) -> DispatchTimeoutResult {
        signal.wait(timeout: .now() + timeout)
    }

    private nonisolated func buildEnvironment() -> [String: String] {
        var environment = ProcessInfo.processInfo.environment

        let currentPath = environment["PATH"] ?? ""
        let commonPaths = [
            "/opt/homebrew/bin",
            "/opt/homebrew/sbin",
            "/usr/local/bin",
            "/usr/local/sbin",
            "/usr/bin",
            "/bin",
            "/usr/sbin",
            "/sbin",
        ]

        let mergedPaths = (currentPath.split(separator: ":").map(String.init) + commonPaths)
            .reduce(into: [String]()) { partialResult, path in
                if !partialResult.contains(path) {
                    partialResult.append(path)
                }
            }

        environment["PATH"] = mergedPaths.joined(separator: ":")
        return environment
    }

    private nonisolated func resolveCommand(named commandName: String) -> String? {
        if commandName.contains("/") {
            return commandName
        }

        let fileManager = FileManager.default
        let searchPaths = (buildEnvironment()["PATH"] ?? "")
            .split(separator: ":")
            .map(String.init)

        for searchPath in searchPaths {
            let candidate = URL(fileURLWithPath: searchPath)
                .appendingPathComponent(commandName)
                .path
            if fileManager.isExecutableFile(atPath: candidate) {
                return candidate
            }
        }

        return nil
    }
}

private final class OutputBuffer: @unchecked Sendable {
    private let lock = NSLock()
    private var data = Data()

    func append(_ newData: Data) {
        guard !newData.isEmpty else { return }
        lock.lock()
        data.append(newData)
        lock.unlock()
    }

    func snapshot() -> Data {
        lock.lock()
        let copy = data
        lock.unlock()
        return copy
    }
}
