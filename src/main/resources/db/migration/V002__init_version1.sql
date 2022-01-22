CREATE TABLE version1 (
    id  uuid PRIMARY KEY,
    major int,
    minor int,
    micro int,
    build int
);

CREATE INDEX version1_major_idx ON version1 (major DESC);
CREATE INDEX version1_major_minor_idx ON version1 (major DESC, minor DESC);
CREATE INDEX version1_major_minor_micro_idx ON version1 (major DESC, minor DESC, micro DESC);
CREATE INDEX version1_major_minor_micro_build_idx ON version1 (major DESC, minor DESC, micro DESC, build DESC);
