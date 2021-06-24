# fred-fdw
Postgres Foreign Data Wrapper for FRED, powered by [fredio](https://github.com/bgrams/fredio) and [multicorn](https://github.com/Segfault-Inc/Multicorn).

### Setup
```bash
$ POSTGRES_PASSWORD=<password> docker-compose up -d
Creating network "fred-fdw_default" with the default driver
Creating fred-fdw ... done

$ docker exec -it fred-fdw bash

$ psql -U postgres -Wf sql/setup.sql -v server=fred -v schema=public
CREATE EXTENSION
CREATE SERVER
IMPORT FOREIGN SCHEMA

$ psql -U postgres -Wc \\d
                   List of relations
 Schema |        Name        |     Type      |  Owner
--------+--------------------+---------------+----------
 public | category           | foreign table | postgres
 public | release            | foreign table | postgres
 public | series             | foreign table | postgres
 public | series_observation | foreign table | postgres
 public | series_updates     | foreign table | postgres
(5 rows)

$ psql -U postgres -Wc "create user mapping for <user> server fred options ( api_key '<fred api key>' );"
```

### Features
* Request concurrency and rate limiting managed by fredio
* Full text search for id's and titles against the `series` table

### Limitations
* Rate limiting is not managed across user processes (i.e. connections)
* Both `series` and `series_observation` tables require `id` or `title`, and `series_id` predicates, respectively.
  In other words full table scans are (reasonably) not supported.
* There may be *serious* performance degradation vs. the pure Python client for reasons that are currently unknown (tested in PG12).
* Joins across foreign tables are not perfect due to the limited control we have over the query optimizer. When performing a join, it may be
necessary to materialize an intermediate result set using a CTE to avoid repetitive API calls e.g.

```sql
WITH rec AS MATERIALIZED (
  SELECT id AS series_id, title, last_updated, units_short
  FROM series WHERE id LIKE 'USREC%'
)
SELECT rec.*, obs.date, obs.value
FROM series_observation obs
JOIN rec USING (series_id)
```

### Should I use this in production?
Probably not, but it's fun to play with. PR's to improve performance or stability are more than welcome!
