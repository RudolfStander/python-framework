CREATE TABLE IF NOT EXISTS Example (
    Id text NOT NULL,
    Tmstamp timestamp NOT NULL,
    LastUpdated timestamp NOT NULL
);

ALTER TABLE Example
  ADD CONSTRAINT EXAMPLE_PKEY PRIMARY KEY (
    Id
  );
