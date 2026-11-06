-- realtime view keyspace. the replication factor token is replaced by the deploy script from
-- the profile, one on dev and three on demo, so the same file serves both.
CREATE KEYSPACE IF NOT EXISTS airpulse
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': __REPLICATION_FACTOR__};
