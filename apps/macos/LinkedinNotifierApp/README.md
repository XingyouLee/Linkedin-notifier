# LinkedinNotifierApp

Standalone macOS SwiftUI client for the cloud-hosted LinkedIn notifier stack.

## Current scope

- Read the remote business database directly from Swift
- Show queue summary and latest batch progress
- Browse profile dashboards and job detail records
- Search jobs by id, title, company, profile, or matched term
- Open the external Airflow UI in the browser

## How it works

- The app uses `PostgresNIO` to connect directly to the remote `JOBS_DB_URL`
- It is intentionally read-only: no local Docker, Astro, or DAG-triggering workflow remains in the UI
- Airflow links open in the default browser using a separately configured external URL

## Open in Xcode

1. Open Xcode
2. Choose `File -> Open`
3. Open [`Package.swift`](/Users/levi/Linkedin-notifier/apps/macos/LinkedinNotifierApp/Package.swift)
4. Run the `LinkedinNotifierApp` scheme

## Build a Local .app Bundle

Run:

```bash
./scripts/package_app.sh
```

The packaging script reads cloud config from the repository root `.env` by default. Required value:

```bash
JOBS_DB_URL=postgresql://user:password@host:5432/jobsdb
```

Optional value:

```bash
LINKEDIN_NOTIFIER_AIRFLOW_WEB_URL=https://your-airflow-host.example.com
```

You can override the env file path with `LINKEDIN_NOTIFIER_ENV_FILE=/abs/path/to/.env ./scripts/package_app.sh`.

If you want to rebuild and immediately relaunch the packaged app in `dist/`, run:

```bash
./scripts/package_and_open.sh
```

That creates:

[`dist/LinkedinNotifierApp.app`](/Users/levi/Linkedin-notifier/apps/macos/LinkedinNotifierApp/dist/LinkedinNotifierApp.app)

You can then drag that `.app` to your Desktop or `Applications` and launch it without opening Xcode.

## Notes

- The packaged `.app` embeds the remote DB URL and optional Airflow web URL at build time, so rebuild it whenever those values change
- `swift test --package-path .` covers the Swift-side config and model behavior, but it does not hit the live database
- Xcode `Build` updates Swift build products, but it does not refresh `dist/LinkedinNotifierApp.app`; use `./scripts/package_app.sh` when you want the packaged app updated
