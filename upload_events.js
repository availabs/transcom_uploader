var promise = require('bluebird');
var http = require("http")
var json2csv = require('json2csv');
var Readable = require('stream').Readable
var copyFrom = require('pg-copy-streams').from;

var db_info = require('./DB_INFO.json')

var options = {
  promiseLib: promise
};
var cn = db_info

var pgp = require('pg-promise')(options);
var db = pgp(cn);

// var eventParams = {
//   /*
//   * YYYY/M/D
//   */
//   endDateTime:"2016/11/01 23:59:00",
//   startDateTime:"2016/10/01 00:00:00" 
// }

var eventParams = {
  endDateTime:"2017/9/14 23:59:00",
  startDateTime:"2017/9/14 00:00:00" 
}

getEventsFromAPI(eventParams)

/*
*
* Receives data from "parseData"
* Receives as CSV
* Creates Temp Table, copy into that. Then insert into real table, resolving conflicts as necessary 
*
*/

function copyIntoDb(csv){
  db.connect()
  .then(function (con) {
    console.log("using copy library")
    var client = con.client;

    function copyFromTemp(err, secondParam){
      console.log("copyFromTemp", err, secondParam)

      var copyRows = client.query(`
        INSERT INTO transcom_events
        SELECT DISTINCT ON (event_id) *
        FROM transcom_temp
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
          event_category = EXCLUDED.event_category
      `)        

      copyRows.on("end",function(error, param1){
        console.log("end of copy rows", error, param1)

        var dropTemp = client.query('drop table transcom_temp;')

        dropTemp.on("end", function(){
          console.log("table has been dropped")
          con.done()
        })
      })

    }

    function populateTemp(err, secondParam){
      console.log("populateTemp", err, secondParam)
      var stream = client.query(copyFrom('COPY transcom_temp FROM STDIN WITH (FORMAT csv, DELIMITER (\'\t\') , FORCE_NULL("close_time"))  '));

      var fileStream = new Readable
      fileStream.push(csv)    // the string you want
      fileStream.push(null)      // indicates end-of-file basically - the end of the stream
      fileStream.on('error', errorDone);
      stream.on('error', errorDone);
      stream.on('end', copyFromTemp);
      fileStream.pipe(stream);
    }

    var createTable = client.query(`
      DROP table if exists transcom_temp; 
      CREATE TEMP TABLE transcom_temp 
      AS
      SELECT * 
      FROM public.transcom_events
      WITH NO DATA;
    `)

    createTable.on("end", populateTemp)
    createTable.on("error", errorDone)

    function errorDone(err){
      console.log("ERROR: ", err)
    }
  }) 
}

/*
* CONVERTS JSON OF DATA TO CSV
* Receives data from "getEventsFromAPI" in JSON format
* Sends "CSV" to "copyIntoDb"
*/

function parseData(data){
  var parsedData = JSON.parse(data)['list']
  var newData = []
  console.log(parsedData.length)
  parsedData.forEach((row,index) => {
    var newRow = {}

    newRow['event_id'] = row['id']
    newRow['event_type'] = row['eventType']
    newRow['facility'] = row['facility']
    newRow['creation'] = row['startDateTime']
    newRow['open_time'] = row['lastUpdate']
    newRow['close_time'] = row['manualCloseDate']
    newRow['duration'] =  row['eventDuration']
    newRow['description'] = row['summaryDescription']
    newRow['from_city'] = row['FromCity']
    newRow['from_count'] = row['county']
    newRow['to_city'] = row['ToCity']
    newRow['state'] = row['state']
    newRow['from_mile_marker'] = row['PrimaryMarker']
    newRow['to_mile_marker'] = row['secondaryMarker']
    newRow['latitude'] = row['pointLAT']
    newRow['longitude'] = row['pointLON']
    newRow['event_category'] = ""

    newData.push(newRow)
  }) 

  var fields = ['event_id', 'event_type', 'facility', 'creation', 'open_time', 'close_time', 'duration', 'description', 'from_city', 'from_count', 'to_city', 'state', 'from_mile_marker', 'to_mile_marker', 'latitude', 'longitude', 'event_category'];
  var result = json2csv({ del:"\t",quotes:'' ,data: newData, fields: fields, hasCSVColumnTitle:false });

  copyIntoDb(result)
}

/*
* 
* GET NEW EVENTS FROM API
* Sends JSON of events to parseData
*
*/

function getEventsFromAPI(params){
  var searchInputs = {}
  var postData = {}

  searchInputs = {
    city:"",
    county:"",
    direction:"",
    endDateTime:params["endDateTime"],
    eventDuration:"",
    eventId:"",
    eventState:"",
    eventType:"",
    eventTypeDesc:"",
    facility:"",
    orgID:"15",
    primaryLoc:"",
    reportingOrg:"",
    secondaryLoc:"",
    startDateTime:params["startDateTime"],
    state:"NY"
  }

  postData = JSON.stringify(searchInputs)
  console.log(postData)
  const options = {
    hostname: 'xcmdfe1.xcmdata.org',
    port: 80,
    path: '/EventSearchNew/xcmEvent/getEventGridData',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Content-Length': Buffer.byteLength(postData)
    }
  };
  var start = new Date();
  var index = 0;
  const myReq = http.request(options, (response) => {
    response.setEncoding('utf8');
    var myBody = [];
    response.on('data', (chunk) => {
      index++;
      //console.log("chunk #",index)
      myBody.push(chunk.toString())

    });
    response.on('end', () => {
      var result = myBody.join('')

      var end = new Date() - start
      console.log(('Request took: '+ end + 'ms'))    
      parseData(result)
    });
  });

  myReq.on('error', (e) => {
    console.error(`problem with request: ${e.message}`);
  });

  // write data to request body
  myReq.write(postData);
  myReq.end();    
}
