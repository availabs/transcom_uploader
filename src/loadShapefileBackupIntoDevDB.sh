#!/bin/bash

# set -e
set -a

pushd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null

source '../config/postgres.env.dev'

BACKUP_FILE_PATH='../sql/ny.inrix_shapefile_20171107.sql.gz'

psql -c 'CREATE SCHEMA IF NOT EXISTS "ny";'
psql -f <(zcat "$BACKUP_FILE_PATH") &> db_restoration_output.txt

popd >/dev/null
