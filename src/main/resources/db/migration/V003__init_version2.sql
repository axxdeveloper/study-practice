CREATE TABLE version2 (
    id  uuid PRIMARY KEY,
    build int
);

CREATE INDEX version2_order_idx ON version2 (build DESC);
