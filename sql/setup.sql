CREATE EXTENSION IF NOT EXISTS multicorn;

CREATE SERVER IF NOT EXISTS :server
FOREIGN DATA WRAPPER multicorn
OPTIONS ( wrapper 'fred_fdw.FDWManager' );

IMPORT FOREIGN SCHEMA fred  -- schema name is arbitrary
FROM SERVER :server
INTO :schema;
