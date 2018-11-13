#!/usr/bin/env node

/* eslint no-console: 0 */

const { join, isAbsolute } = require('path');
const json2csv = require('json2csv');
const envFile = require('node-env-file');
const { readFileSync } = require('fs');
const { Readable } = require('stream');
const copyFrom = require('pg-copy-streams').from;

const configFile =
  process.env.NODE_ENV === 'production'
    ? 'postgres.env.prod'
    : 'postgres.env.dev';

envFile(join(__dirname, '../config/', configFile));

const { Client } = require('pg');

const client = new Client();

const inputFileRelPath = process.argv[2];

if (!inputFileRelPath) {
  console.error('USAGE: Specify the input file as the first CLI argument.');
  process.exit(1);
}

const inputFilePath = isAbsolute(inputFileRelPath)
  ? inputFileRelPath
  : join(process.cwd(), inputFileRelPath);

const TMP_TABLE = `tmp_transcom_${new Date().getTime()}`;

const COLUMNS = [
  'event_id',
  'event_type',
  'facility',
  'creation',
  'open_time',
  'close_time',
  'duration',
  'description',
  'from_city',
  'from_count',
  'to_city',
  'state',
  'from_mile_marker',
  'to_mile_marker',
  'latitude',
  'longitude',
  'event_category',
  'point_geom'
];

const parseData = inputData => {
  const newData = [];

  inputData.forEach(row => {
    const newRow = {};

    newRow.event_id = row.id;
    newRow.event_type = row.eventType;
    newRow.facility = row.facility;
    newRow.creation = row.startDateTime;
    newRow.open_time = row.lastUpdate;
    newRow.close_time = row.manualCloseDate;
    newRow.duration = row.eventDuration;
    newRow.description = row.summaryDescription;
    newRow.from_city = row.FromCity;
    newRow.from_count = row.county;
    newRow.to_city = row.ToCity;
    newRow.state = row.state;
    newRow.from_mile_marker = row.PrimaryMarker;
    newRow.to_mile_marker = row.secondaryMarker;
    newRow.latitude = row.pointLAT;
    newRow.longitude = row.pointLON;
    newRow.event_category = '';
    newRow.point_geom = '';

    newData.push(newRow);
  });

  const result = json2csv({
    del: '\t',
    quotes: '',
    data: newData,
    COLUMNS,
    hasCSVColumnTitle: false
  });

  return result;
};

const createTempTable = () =>
  client.query(`
  DROP table if exists ${TMP_TABLE};

  CREATE TEMP TABLE ${TMP_TABLE}
  AS
    SELECT *
      FROM public.transcom_events
      WITH NO DATA;
  `);

const populateTempTable = csv =>
  new Promise((resolve, reject) => {
    const stream = client.query(
      copyFrom(
        `COPY ${TMP_TABLE} (${COLUMNS}) FROM STDIN WITH (FORMAT csv, DELIMITER ('\t') , FORCE_NULL("close_time"));`
      )
    );

    const fileStream = new Readable();
    fileStream.push(csv); // the string you want
    fileStream.push(null); // indicates end-of-file basically - the end of the stream
    fileStream.on('error', reject);
    stream.on('error', reject);
    stream.on('end', resolve);
    fileStream.pipe(stream);
  });

const setEventCategory = () =>
  client.query(`
      UPDATE ${TMP_TABLE} AS t
        SET
          event_category = e.event_category,
          point_geom = ST_MakePoint(t.longitude, t.latitude)::geography::geometry
        FROM
          transcom_events AS e
        WHERE (
          (e.event_type = t.event_type)
          AND
          (e.event_category != 'null')
        )
    `);

const copyFromTemp = () =>
  client.query(`
    INSERT INTO transcom_events
      SELECT DISTINCT ON (event_id) *
          FROM ${TMP_TABLE}
      ON CONFLICT ON CONSTRAINT transcom_events_pkey DO UPDATE
        SET
          event_id = EXCLUDED.event_id,
          event_type = EXCLUDED.event_type,
          facility = EXCLUDED.facility,
          creation = EXCLUDED.creation,
          open_time = EXCLUDED.open_time,
          close_time = EXCLUDED.close_time,
          duration = EXCLUDED.duration,
          description = EXCLUDED.description,
          from_city = EXCLUDED.from_city,
          from_count = EXCLUDED.from_count,
          to_city = EXCLUDED.to_city,
          state = EXCLUDED.state,
          from_mile_marker = EXCLUDED.from_mile_marker,
          to_mile_marker = EXCLUDED.to_mile_marker,
          latitude = EXCLUDED.latitude,
          longitude = EXCLUDED.longitude,
          event_category = EXCLUDED.event_category,
          point_geom = EXCLUDED.point_geom ;`);

const dropTempTable = () => client.query(`DROP TABLE ${TMP_TABLE};`);

const finishUp = () => client.query('VACUUM ANALYZE transcom_events;');

(async () => {
  await client.connect();
  const { list: inputData } = JSON.parse(readFileSync(inputFilePath));

  if (!Array.isArray(inputData)) {
    throw new Error('inputData list field is not an array');
  }

  const parsedData = parseData(inputData);

  await createTempTable();
  await populateTempTable(parsedData);
  await setEventCategory();

  await copyFromTemp();

  await dropTempTable();

  await finishUp();

  client.end();
})().catch(err => {
  console.error(err);
  client.end();
});
