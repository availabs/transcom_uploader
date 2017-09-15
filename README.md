### STEPS TO RUN:
1) npm install
2) create "DB_INFO.json" with DB info 
3) node upload_events (assumes public.transcom_events table in npmrds_api)

NOTES:

Default start/end date is TODAY
Can modify the default dates within the file itself instead of using command line arguments.
Start-date cannot be after today's date.
End-date can be after today's date, but will effectively be today's date.
Time of day cannot be specified via command line -- start is set at 00:00:00, end at 23:59:00. 