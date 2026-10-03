#!/usr/bin/env bash
set -euo pipefail
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 --set=app_password="$APP_DB_PASSWORD" \
  --set=connector_password="$CONNECTOR_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE pkb_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD %L', :'app_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='pkb_app') \gexec
SELECT format('CREATE ROLE pkb_connector LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD %L', :'connector_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='pkb_connector') \gexec
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO pkb_app;
GRANT USAGE ON SCHEMA public TO pkb_connector;
SQL
