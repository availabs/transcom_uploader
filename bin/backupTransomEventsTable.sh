#!/bin/bash

set -e
set -a

pushd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null

SQL_DIR='../sql'
mkdir -p "$SQL_DIR"

source '../config/postgres.env.prod'

DATESTAMP="$(date +'%Y%m%dT%H%M%S')"

pg_dump \
  --table=public.transcom_events --no-owner |
  gzip -9 \
  > "${SQL_DIR}/transcom_events.${DATESTAMP}.sql.gz"

popd >/dev/null
