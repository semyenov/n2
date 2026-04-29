CREATE TABLE IF NOT EXISTS profile_provider_migrations (
  migration_id integer PRIMARY KEY,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  name text NOT NULL
);

CREATE TABLE IF NOT EXISTS request_provider_migrations (
  migration_id integer PRIMARY KEY,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  name text NOT NULL
);
