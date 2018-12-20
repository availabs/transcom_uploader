#!/usr/bin/env node

const { spawnSync } = require('child_process');
const { join } = require('path');

const scriptPath = join(__dirname, '../src/uploadTranscomData.js');

const yrmos = [[2017, 10], [2017, 11], [2017, 12]].concat(
  [...Array(11)].map((_, i) => [2018, i + 1])
);

for (let i = 0; i < yrmos.length; i += 1) {
  const [year, month] = yrmos[i];
  const startDate = new Date(year, month - 1, 1);
  const endDate = new Date(year, month, 0);

  const YYYY = startDate.getFullYear();
  const MM = `0${startDate.getMonth() + 1}`.slice(-2);
  let DD = `0${startDate.getDate()}`.slice(-2);

  const startDateTime = `${YYYY}/${MM}/${DD} 00:00:00`;

  DD = `0${endDate.getDate()}`.slice(-2);
  const endDateTime = `${YYYY}/${MM}/${DD} 23:59:59`;

  // console.log(startDateTime, endDateTime);

  const { stdout, stderr } = spawnSync(
    scriptPath,
    [`--startDateTime=${startDateTime}`, `--endDateTime=${endDateTime}`],
    { encoding: 'utf8' }
  );

  console.log('='.repeat(10), `${YYYY}/${MM}`, '='.repeat(10));
  console.log('STDOUT:', stdout);
  console.log('STDERR:', stderr);
  console.log();
}
