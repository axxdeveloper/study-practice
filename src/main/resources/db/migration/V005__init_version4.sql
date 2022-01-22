
CREATE TABLE version4 (
    id  uuid PRIMARY KEY,
    build VARCHAR 
);

CREATE INDEX version4_build_idx ON version4 (build DESC);
