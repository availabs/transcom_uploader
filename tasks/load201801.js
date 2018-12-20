#!/usr/bin/env node

// 2018 |           1 |   245
// 2018 |           2 |    83
// 2018 |           4 |   376

const { spawnSync } = require('child_process');
const { join } = require('path');

const NODE_ENV = 'development';

const scriptPath = join(__dirname, '../src/uploadTranscomData.js');

const yrmos = [[2018, 1], [2018, 2], [2018, 4]];

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
    { encoding: 'utf8', env: { NODE_ENV } }
  );

  console.log('='.repeat(10), `${YYYY}/${MM}`, '='.repeat(10));
  console.log('STDOUT:', stdout);
  console.log('STDERR:', stderr);
  console.log();
}
