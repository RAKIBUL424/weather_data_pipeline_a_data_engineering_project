CREATE USER superset WITH PASSWORD 'superset' SUPERUSER;
CREATE DATABASE superset_db OWNER superset;
CREATE USER examples WITH PASSWORD 'examples' SUPERUSER;
CREATE DATABASE examples_db OWNER examples;