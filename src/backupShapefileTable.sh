#!/bin/bash

set -e
set -a

pushd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null

SQL_DIR='../sql'
mkdir -p "$SQL_DIR"

source '../config/postgres.env.prod'

pg_dump \
  --table=public.inrix_shapefile --table=ny.inrix_shapefile_20171107 --no-owner |
  gzip -9 \
  > "${SQL_DIR}/ny.inrix_shapefile_20171107.sql.gz"

popd >/dev/null
