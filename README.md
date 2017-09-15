1) npm install
2) create "DB_INFO.json" with DB info 
3) edit "eventParams" variable in "upload events" to modify date range
4) node upload_events (assumes transcom_events table)

NOTES:
Default start/end date is TODAY
Start-date cannot be after today's date.
End-date can be after today's date, but will effectively be today's date
Time of day cannot be specified via command line -- start is set at 00:00:00, end at 23:59:00