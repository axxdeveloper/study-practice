===== api =====
1. Use Flyway to initialize DB schema
2. Initialize schema (Treat api as the schema owner)
    - event_time and event_id don't need to be unique
    - event_time and event_id need to be indexed for API querying
3. Use MyBatis to query data and return to API client


==== spark ====
1. group events by tumbling window (1 minute) for counting
2. format timestamp to a specific time format
3. persist data to DB through jdbc format


==== millions of events ====
1. when there are millions of events, executors may handle events which happened at the same time, 
2. executors calculate the batched dataset and insert a new counting row to DB 
   (event won't be counted multiple times)
3. api can group all counts


==== Execution ====
1. Start HsqlDB
2. Start ApiApp (Initialize schema for the first run)
3. Start SparkApp
4. Query
    4.1 Query count by time example 
        curl http://localhost:8080/api/counters/time/202205151705
        [{"31":1},{"64":1},{"8":2},{"62":2},{"32":3},{"14":1},{"65":1},{"16":1},{"96":1},{"30":2},{"17":1},{"91":2},{"18":1},{"61":1},{"67":1},{"38":1},{"28":1},{"4":1},{"11":1},{"5":3},{"20":1},{"72":1},{"89":1},{"54":1},{"0":1},{"68":1},{"71":1},{"69":1},{"59":2},{"23":1},{"40":1},{"93":1},{"42":1},{"3":1},{"44":1},{"94":1},{"37":1},{"29":1},{"52":1},{"41":1},{"57":1},{"34":1}]
    4.2 Query count by time and eventId example
        curl http://localhost:8080/api/counters/time/202205151648/eventId/27
        1
