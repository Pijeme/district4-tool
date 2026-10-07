# District 4 Tool

A Flask application served by Gunicorn, with Google Sheets mirrors, Google Drive
libraries, Gemini assistance, and SQLite storage. No separate database server is
needed. Run one container: background jobs and their progress use process-local
state, so the image deliberately runs one Gunicorn worker with four threads.

## Build and publish from your development machine

Run these commands inside `district4-tool`. Keep your existing `.env` and add
`DOCKER_IMAGE` and `IMAGE_TAG`, or create `.env` from `.env.example` on a fresh
checkout. Set `DOCKER_IMAGE` to the Docker Hub repository you own; its default is
`pijeme/district4-tool-main`. Choose a version tag for each release.

```dotenv
DOCKER_IMAGE=pijeme/district4-tool-main
IMAGE_TAG=1.0.0
```

```sh
docker login
docker build -t pijeme/district4-tool-main:1.0.0 .
docker push pijeme/district4-tool-main:1.0.0
```

Use your own repository and tag in both commands, matching `DOCKER_IMAGE` and
`IMAGE_TAG` in `.env`. Build directly with the Dockerfile on your development
machine. The project's single `compose.yml` pulls the published image, so the
server only needs `compose.yml`, `.env` and `service_account.json`.

The Dockerfile installs the pinned dependencies in `requirements.lock.txt`. It
copies application code, templates and static assets only. Credentials, local
databases, reports and caches are excluded from the image. The application runs
as UID/GID 10001, and logs go to Docker with size limits.

For a Linux server with a different CPU architecture, build explicitly. To
publish one image supporting both amd64 and arm64:

```sh
docker buildx create --name district4-builder --driver docker-container --use
docker buildx build --platform linux/amd64,linux/arm64 --tag YOUR_USERNAME/district4-tool:1.0.0 --push .
```

## Deploy with three files

Your server needs Docker Engine and the Docker Compose plugin. Create a directory
and put only these files inside it:

```text
district4/
  compose.yml
  .env
  service_account.json
```

Copy the repository's `compose.yml`. Create `.env` using `.env.example` as a
reference. Fill in:

- `DOCKER_IMAGE`: your Docker Hub repository, without a tag or URL prefix.
- `IMAGE_TAG`: the tag you published; defaults to `latest`.
- `FLASK_SECRET_KEY`: a long random secret; keep it stable across updates.
- `GEMINI_API_KEY`: required by the application at startup.
- `PLEDGE_SCRIPT_URL` and `PLEDGE_API_TOKEN`: if you use Thanksgiving Pledges.
- `TEMP_EDIT_USER_TOKEN` and `TEMP_EDIT_ADMIN_TOKEN`: set private values if you use
  temporary editing. Blank values disable access to the corresponding routes.

Generate a Flask secret on your development machine:

```sh
python -c "import secrets; print(secrets.token_hex(32))"
```

Copy your existing `service_account.json` to the server directory, or paste its
complete JSON contents into a file with that name. Compose mounts the file
read-only at `/app/service_account.json` and configures the application to use
it. Google Sheets and Drive features require valid service account credentials.
Share the spreadsheet and library folders with that service account's email.

If you previously set `GOOGLE_SERVICE_ACCOUNT_BASE64` or
`GOOGLE_SERVICE_ACCOUNT_JSON` in `.env`, remove those entries to use the mounted
file. The file must exist before starting Compose; a missing file causes an
error instead of creating a directory. Keep it readable by the container's
UID 10001. The credential file is excluded from Git and the Docker image.

For a private Docker Hub repository, run `docker login` on the server once.
Then, from the server project directory:

```sh
docker compose pull
docker compose up -d
docker compose ps
```

The default URL is `http://SERVER_IP:5000`. Change `APP_PORT` in `.env` to use a
different host port. If an existing local reverse proxy serves the application,
set `APP_BIND_ADDRESS=127.0.0.1`. The health endpoint is `/healthz`; it checks
the web process without calling Google services. Inspect logs with
`docker compose logs --tail=100 -f district4`.

No checkout, Dockerfile, Python installation, or database files are needed on
the server for a fresh deployment. The first application
request creates the SQLite schema and bootstraps the Sheets mirrors. With a
fresh volume, local-only records and Drive library indexes start empty; use the
app's sync tools to populate libraries. Existing local-only data requires the
one-time migration below.

## Persistent data and updates

Compose creates a named volume called `<project-name>_district4-data` at `/data`.
It contains `app_v2.db`, `ai_index.db`, `pastor_resource_sync_state.db`, SQLite
journal/lock files, `generated_reports/`, and `book_thumbnail_cache/`. New images
reuse this volume. Docker documents this [volume lifecycle](https://docs.docker.com/engine/storage/volumes/).

For each update, publish the new image, change `IMAGE_TAG` if necessary, then:

```sh
docker compose pull
docker compose up -d
```

Keep the server directory/project name stable so Compose selects the same data
volume. `docker compose down` retains it; `docker compose down -v` deletes it.

## One-time migration from the old deployment

Stop the old application before collecting its data so SQLite journals are not
changing. Keep a backup of the old files. Copy its databases and matching
`-wal`/`-shm` files, if present, plus reports and thumbnail cache into a temporary
`migration/` directory on the server. Do not put credentials in that directory.

For a new, empty deployment volume:

```sh
docker compose pull
docker compose create district4
docker compose cp ./migration/. district4:/data/
docker compose run --rm --no-deps --user 0 district4 chown -R 10001:10001 /data
docker compose up -d
```

Copying data is needed once when retaining existing records. It is not part of
normal deployments. Do not import into an active or already populated volume.

## Backup and restore

For a consistent full backup, stop the application briefly and copy the volume:

```sh
docker compose stop district4
docker compose cp district4:/data ./district4-backup
docker compose start district4
```

To restore a backup into an empty volume, use the migration commands above with
`./district4-backup/.` as the source. Back up before updates that change schemas;
an older image may not support a database upgraded by a newer release.
