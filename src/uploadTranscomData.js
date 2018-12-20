#!/usr/bin/env node

/* eslint no-console: 0 */

const { join } = require('path');
const json2csv = require('json2csv');
const envFile = require('node-env-file');
const request = require('request-promise-native');
const { Readable } = require('stream');
const copyFrom = require('pg-copy-streams').from;
const minimist = require('minimist');
const { Client } = require('pg');

const eventTypes2Categories = require('./eventTypes2Categories');

const argv = minimist(process.argv.slice(2), { boolean: true });
console.log(JSON.stringify(argv, null, 4));

const configFile =
  process.env.NODE_ENV === 'production'
    ? 'postgres.env.prod'
    : 'postgres.env.dev';

envFile(join(__dirname, '../config/', configFile));

const client = new Client();

const TRANSCOM_URI =
  'https://xcmdfe1.xcmdata.org/EventSearchNew/xcmEvent/getEventGridData';

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

const getLatestStartTime = async () => {
  const q =
    "SELECT to_char(MAX(creation), 'YYYY/MM/DD HH24:MI:SS') AS latest FROM transcom_events;";

  const {
    rows: [{ latest }]
  } = await client.query(q);
  return latest;
};

const getLatestUpdateTime = async () => {
  const q =
    "SELECT to_char(MAX(open_time), 'YYYY/MM/DD HH24:MI:SS') AS latest FROM transcom_events;";

  const {
    rows: [{ latest }]
  } = await client.query(q);
  return latest;
};

const getEventsFromAPI = async (startDateTime, endDateTime) => {
  const options = {
    method: 'POST',
    uri: TRANSCOM_URI,
    body: {
      orgID: '15',
      startDateTime,
      endDateTime,
      state: 'NY'
    },
    json: true // Automatically stringifies the body to JSON
  };

  return request(options);
};

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
    newRow.event_category =
      eventTypes2Categories[row.eventType.toLowerCase()] || 'other';
    newRow.point_geom = '';

    newData.push(newRow);
  });

  const result = json2csv({
    del: '\t',
    // quotes: '',
    defaultValue: '',
    data: newData,
    COLUMNS,
    hasCSVColumnTitle: false
  });
  require('fs').writeFileSync('data.csv', result);

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
        `COPY ${TMP_TABLE} (${COLUMNS})
          FROM STDIN WITH (
            FORMAT csv,
            DELIMITER ('\t'),
            FORCE_NULL(
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
            )
          );`
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

const setPointGeom = () =>
  client.query(`
      UPDATE ${TMP_TABLE}
        SET point_geom = ST_MakePoint(longitude, latitude)::geography::geometry
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

const mapEventsToTMCs = async latestUpdateTime => {
  const sql = `
    BEGIN;

    DROP TABLE IF EXISTS tmp_buffered_event_pts;
    CREATE TEMPORARY TABLE tmp_buffered_event_pts
    AS
      SELECT
          event_id,
          point_geom,
          ST_Buffer(GEOGRAPHY(point_geom), 75) AS buffered_pt
        FROM transcom_events
        WHERE (
          (close_time >= '${latestUpdateTime ||
            '1900/01/01 00:00:00'}'::TIMESTAMP)
          AND
          (tmc IS NULL)
        )
    ;

    CREATE INDEX tmp_buffered_event_pts_idx ON tmp_buffered_event_pts USING GIST (buffered_pt);
    CLUSTER tmp_buffered_event_pts USING tmp_buffered_event_pts_idx;
    ANALYZE tmp_buffered_event_pts;

    DROP TABLE IF EXISTS tmp_buffered_tmcs;
    CREATE TEMPORARY TABLE tmp_buffered_tmcs
    AS
      SELECT
          tmc,
          wkb_geometry,
          ST_Buffer(GEOGRAPHY(wkb_geometry), 75) AS buffered_tmc
        FROM ny.inrix_shapefile_20171107
    ;

    CREATE INDEX tmp_buffered_tmcs_idx ON tmp_buffered_tmcs USING GIST (buffered_tmc);
    CLUSTER tmp_buffered_tmcs USING tmp_buffered_tmcs_idx;
    ANALYZE tmp_buffered_tmcs;

    DROP TABLE IF EXISTS tmp_events_to_tmcs;
    CREATE TEMPORARY TABLE tmp_events_to_tmcs
    AS
    SELECT
        event_id,
        MIN(shp.tmc) AS tmc
      FROM tmp_buffered_event_pts AS te
        INNER JOIN (
          SELECT
              event_id,
              MIN(ST_Distance(te.point_geom, shp.wkb_geometry))
                OVER (PARTITION BY event_id) AS min_dist
            FROM tmp_buffered_event_pts AS te
              JOIN tmp_buffered_tmcs AS shp
              ON (te.buffered_pt && shp.buffered_tmc)
          ) AS sub_min_dists USING (event_id)
        INNER JOIN tmp_buffered_tmcs AS shp
          ON (te.buffered_pt && shp.buffered_tmc)
        WHERE (ST_Distance(te.point_geom, shp.wkb_geometry) = sub_min_dists.min_dist)
        GROUP BY event_id
    ;

    UPDATE transcom_events
      SET tmc = tmp_events_to_tmcs.tmc 
        FROM tmp_events_to_tmcs
        WHERE (
          (transcom_events.event_id = tmp_events_to_tmcs.event_id)
          AND
          (transcom_events.tmc is null)
        )
    ;

    COMMIT;
  `;

  await client.query(sql);
};

const finishUp = () => client.query('VACUUM ANALYZE transcom_events;');

(async () => {
  await client.connect();

  const startDateTime = argv.startDateTime || (await getLatestStartTime());
  // const latestUpdateTime = await getLatestUpdateTime();

  const endDateTime = argv.endDateTime || new Date();

  const { list: inputData } = await getEventsFromAPI(
    startDateTime,
    endDateTime
  );

  if (!Array.isArray(inputData)) {
    throw new Error('inputData list field is not an array');
  }

  console.log('# Events:', inputData.length);

  const parsedData = parseData(inputData);

  await createTempTable();
  await populateTempTable(parsedData);
  await setPointGeom();

  await copyFromTemp();

  await dropTempTable();

  // await mapEventsToTMCs(latestUpdateTime);
  await mapEventsToTMCs(null);
  await finishUp();

  client.end();
})().catch(err => {
  console.error(err);
  client.end();
});
