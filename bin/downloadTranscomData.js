#!/usr/bin/env node

const { join } = require('path');
const request = require('request-promise-native');
const { sync: mkdirpSync } = require('mkdirp');
const envFile = require('node-env-file');
const { writeFileSync } = require('fs');

envFile(join(__dirname, '../config/postgres.env.prod'));

const { Client } = require('pg');
const client = new Client();

const dataDir = join(__dirname, '../data');
mkdirpSync(dataDir);

const TRANSCOM_URI =
  'https://xcmdfe1.xcmdata.org/EventSearchNew/xcmEvent/getEventGridData';

const getLatestStartTime = async () => {
  const q = 'SELECT MAX(creation) AS latest FROM transcom_events;';

  const {
    rows: [{ latest }]
  } = await client.query(q);
  return latest;
};

const formatDate = date => {
  const yyyy = date.getFullYear();
  const mm = `0${date.getMonth() + 1}`.slice(-2);
  const dd = `0${date.getDate()}`.slice(-2);
  const hh = `0${date.getDate()}`.slice(-2);
  const MM = `0${date.getMinutes()}`.slice(-2);
  const ss = `0${date.getSeconds()}`.slice(-2);

  return `${yyyy}${mm}${hh}T${hh}${MM}${ss}`;
};

/*
 * GET NEW EVENTS FROM API
 * Sends JSON of events to parseData
 */
const getEventsFromAPI = async (startDateTime, endDateTime) => {
  var options = {
    method: 'POST',
    uri: TRANSCOM_URI,
    body: {
      orgID: '15',
      startDateTime: '2018/11/12 00:00:00',
      endDateTime: '2018/11/12 06:00:00',
      state: 'NY'
    },
    json: true // Automatically stringifies the body to JSON
  };

  return await request(options);
};

const parseEventsDates = events => {
  const [{ startDateTime: seedDate }] = events.list;
  const seed = formatDate(new Date(seedDate));

  const [earliest, latest] = events.list.reduce(
    (acc, { startDateTime }) => {
      const time = formatDate(new Date(startDateTime));
      if (time < acc[0]) {
        acc[0] = time;
      }

      if (time > acc[1]) {
        acc[1] = time;
      }

      return acc;
    },
    [seed, seed]
  );

  return { earliest, latest };
};

(async () => {
  await client.connect();
  const lastestStartTime = await getLatestStartTime();
  const now = new Date();

  const events = await getEventsFromAPI(lastestStartTime, now);

  if (!Array.isArray(events.list)) {
    throw new Error('events.list is not an array');
  }

  const { earliest, latest } = parseEventsDates(events);

  const outputFileName = `transcom_events.${earliest}-${latest}.${formatDate(
    now
  )}.json`;

  writeFileSync(join(dataDir, outputFileName), JSON.stringify(events));
  client.end();
})().catch(err => {
  console.error(err);
  client.end();
});
