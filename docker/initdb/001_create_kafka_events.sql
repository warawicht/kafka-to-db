CREATE TABLE IF NOT EXISTS kafka_events (
    topic text NOT NULL,
    partition int NOT NULL,
    offset bigint NOT NULL,
    key bytea,
    value bytea NOT NULL,
    headers jsonb,
    event_time timestamptz NOT NULL,
    PRIMARY KEY (topic, partition, offset)
);
