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

//Default
var today = new Date()
var eventParams = {
  endDateTime:today.getFullYear() + "/" + (today.getMonth() + 1) + "/" + today.getDate() + " 23:59:00",
  startDateTime:today.getFullYear() + "/" + (today.getMonth() + 1) + "/" + today.getDate() + " 00:00:00" 
}

switch(process.argv.length){
  case 2:
    //use default values
    break;
  case 3:
    console.log("Must use either '-start' or '-end' when only giving one date.")
    return; 
  case 4:
    if(!process.argv[2].includes("-")){
      eventParams["endDateTime"] = process.argv[3] + " 23:59:00"
      eventParams["startDateTime"] = process.argv[2] + " 00:00:00"
    }
    else if(process.argv[2].toLowerCase() == "-start"){
      eventParams["startDateTime"] = process.argv[3] + " 00:00:00"   
    }
    else if(process.argv[2].toLowerCase() == "-end"){
      eventParams["endDateTime"] = process.argv[3] + " 23:59:00" 
    }
    else{
      console.log("Invalid command line parameter:" + process.argv[2])
      return;
    }
    break;
  case 5:
    console.log("Incorrect number of arguments. Specify '-start' or '-end' with a date, or supply two dates")
    return;
  case 6:
    eventParams["endDateTime"] = process.argv[5] + " 23:59:00"
    eventParams["startDateTime"] = process.argv[3] + " 00:00:00"
    break;  
  case 7:
    console.log("Incorrect number of arguments. Specify '-start' or '-end' with a date, or supply two dates")
    return;
}

//Check to make sure end is after start
if(Date.parse(eventParams["endDateTime"]) < Date.parse(eventParams["startDateTime"])){
  console.log("End date must be equal to or after start date")
  return;
}

var startDate = new Date(eventParams["startDateTime"])
//Check to make sure start day is before or equal to today
if(startDate.getMonth() > today.getMonth()){
  console.log("Start date must be before or equal to today's date")
  return;
}
else if(startDate.getMonth() == today.getMonth() && startDate.getDate() > today.getDate()){
  console.log("Start date must be before or equal to today's date")
  return;
}

console.log(eventParams)
//getEventsFromAPI(eventParams)


/*
* Receives data from "parseData"
* Receives as CSV
* Creates Temp Table, copy into that. Then insert into real table, resolving conflicts as necessary 
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
* GET NEW EVENTS FROM API
* Sends JSON of events to parseData
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
