CREATE TABLE IF NOT EXISTS state(
       name text NOT NULL PRIMARY KEY,
       value jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS document(
       id uuid NOT NULL PRIMARY KEY,
       target_version bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS version_mapping(
       id uuid NOT NULL,
       source_version bigint NOT NULL,
       target_version bigint NOT NULL,
       created timestamptz NOT NULL,
       PRIMARY KEY(id, source_version)       
);

CREATE INDEX idx_mapping_created
ON version_mapping (created);

---- create above / drop below ----

DROP TABLE IF EXISTS state;
DROP TABLE IF EXISTS document;
DROP TABLE IF EXISTS version_mapping;
