
CREATE TABLE version3 (
    id  uuid PRIMARY KEY,
    build bytea
);

CREATE INDEX version3_order_idx ON version3 (build DESC); 
