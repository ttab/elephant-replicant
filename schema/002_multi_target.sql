CREATE TABLE replication_target (
       name          text PRIMARY KEY,
       repository_url text NOT NULL,
       oidc_config   text NOT NULL,
       client_id     text NOT NULL,
       client_secret text NOT NULL,
       start_from    bigint NOT NULL DEFAULT 0,
       config        jsonb NOT NULL DEFAULT '{}',
       enabled       boolean NOT NULL DEFAULT true,
       created       timestamptz NOT NULL DEFAULT now(),
       updated       timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE document
      ADD COLUMN target_name text NOT NULL DEFAULT 'default';

ALTER TABLE document
      DROP CONSTRAINT document_pkey,
      ADD PRIMARY KEY (target_name, id);

ALTER TABLE version_mapping
      ADD COLUMN target_name text NOT NULL DEFAULT 'default';

ALTER TABLE version_mapping
      DROP CONSTRAINT version_mapping_pkey,
      ADD PRIMARY KEY (target_name, id, source_version);

UPDATE state SET name = 'default:' || name WHERE name = 'log_state';

CREATE TABLE job_lock (
       name      text NOT NULL PRIMARY KEY,
       holder    text NOT NULL,
       touched   timestamptz NOT NULL,
       iteration bigint NOT NULL
);

---- create above / drop below ----

DROP TABLE IF EXISTS job_lock;

UPDATE state SET name = 'log_state' WHERE name = 'default:log_state';

ALTER TABLE version_mapping
      DROP CONSTRAINT version_mapping_pkey,
      ADD PRIMARY KEY (id, source_version);

ALTER TABLE version_mapping
      DROP COLUMN target_name;

ALTER TABLE document
      DROP CONSTRAINT document_pkey,
      ADD PRIMARY KEY (id);

ALTER TABLE document
      DROP COLUMN target_name;

DROP TABLE IF EXISTS replication_target;
