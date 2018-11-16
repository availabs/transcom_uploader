#!/bin/bash

# set -e
set -a

pushd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null

source '../config/postgres.env.dev'

BACKUP_FILE_PATH="$(find '../sql' -name 'transcom_events.*.sql.gz' | sort | tail -1)"

psql -f <(zcat "$BACKUP_FILE_PATH")

popd >/dev/null
