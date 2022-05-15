CREATE TABLE event_count (
    id UUID PRIMARY KEY,
    event_time varchar (64),
    event_id int NOT NULL,
    count bigint NOT NULL,
);

CREATE INDEX index_event_time
ON event_count (event_time);

CREATE INDEX index_event_time_event_id
ON event_count (event_time, event_id);
