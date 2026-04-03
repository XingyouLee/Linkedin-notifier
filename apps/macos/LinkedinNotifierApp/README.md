# LinkedinNotifierApp

Local macOS SwiftUI frontend for the Astro/Airflow project in this repository.

## MVP scope

- Detect whether Docker is reachable
- Detect whether the local Astro project is running
- Start the local Astro stack
- Trigger the scan and fitting DAGs
- Show recent DAG runs
- Search jobs by id, title, company, profile, or matched term
- Inspect per-profile fit state for a selected job

## How it works

- The app shells out to `docker` and `astro`
- Airflow CLI commands run inside the local scheduler container via `docker exec`
- Business data is fetched from Postgres through [`frontend_data.py`](/Users/levi/Linkedin-notifier/dags/frontend_data.py), which also runs inside the scheduler container

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

If you want to rebuild and immediately relaunch the packaged app in `dist/`, run:

```bash
./scripts/package_and_open.sh
```

That creates:

[`dist/LinkedinNotifierApp.app`](/Users/levi/Linkedin-notifier/apps/macos/LinkedinNotifierApp/dist/LinkedinNotifierApp.app)

You can then drag that `.app` to your Desktop or `Applications` and launch it without opening Xcode.

## Notes

- The app remembers the last project path you entered
- Xcode `Build` updates the Swift build products, but it does not refresh `dist/LinkedinNotifierApp.app`; use `Run` for Xcode testing, or `./scripts/package_app.sh` when you want the packaged app updated
- The packaged `.app` also embeds the current repo path as its initial default, so it can launch correctly outside Xcode on this machine
- If you move the repo later, update the path field in the app once or rebuild the `.app`
- The current implementation expects the local Astro containers to mount `dags/` into `/usr/local/airflow/dags`
