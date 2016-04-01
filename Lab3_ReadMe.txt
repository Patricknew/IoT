Q1.py:
Acquire all data from the MTA feed: tripUpdate, alert & vehicle messages. Some tripIds appear in both the vehicle & tripUpdate message feeds, but they carry different information. The "trip_update" message feed typically carries general route information & the "vehicle" message update carries more 'current position' information. My code incorporates information from both feeds, where applicable.

Q2.py:
It creates a DynamoDB table first if it detects there isn’t one. Then it has 2 tasks running in parallel: 
a. running every 30 sec, adds data to the "mta" DynamoDB table continuously.
b. running every 60 sec, cleans out data from the table that is older than 2 minutes old.
It also handles error conditions, such as, trying to create a table that already exists, overwriting data etc.

Extra1.py and Extra2.py are the corresponding codes for implementing these two codes above on a cloud service. We chose AWS EC2-based Virtual Machines.