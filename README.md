# fred-fdw
Foreign Data Wrapper for FRED, powered by the [fredio](https://github.com/bgrams/fredio) library.

### Setup
```bash
$ POSTGRES_PASSWORD=<password> docker-compose up -d
Creating network "fred-fdw_default" with the default driver
Creating fred-fdw_postgres_1 ... done

$ docker exec -it fred-fdw_postgres_1 bash

bash# psql -U postgres -Wf sql/setup.sql -v server=fred -v schema=public
CREATE EXTENSION
CREATE SERVER
IMPORT FOREIGN SCHEMA

bash# psql -U postgres -Wc \\d
                   List of relations
 Schema |        Name        |     Type      |  Owner
--------+--------------------+---------------+----------
 public | series             | foreign table | postgres
 public | series_observation | foreign table | postgres
(2 rows)

bash# psql -U postgres -Wc "create user mapping for <user> server fred options ( api_key '<fred api key>' );"
```

### Features
* Request concurrency and rate limiting managed by fredio
* Full text search for series id's against the `series` table

### Limitations
* Rate limiting is not managed across user processes (i.e. connections)
* Both `series` and `series_observation` tables require `id`, and `series_id` predicates, respectively.
  In other words full table scans are (reasonably) not supported.
* Joins across foreign tables are clunky since a) predicates are required and b) join filters are not pushed to each
  table by the query planner. A workaround would be to join materialized CTE's that share common filters e.g.
```sql
WITH
    q1 AS MATERIALIZED (SELECT * FROM SERIES WHERE id IN ('USRECD', 'EFFR', 'GDP')),
    q2 AS MATERIALIZED (
        SELECT * FROM series_observation
        WHERE series_id IN ('USRECD', 'EFFR', 'GDP')
    )
SELECT q1.*, q2.date, q2.value
FROM q1 JOIN q2
ON q1.id = q2.series_id
  ```

### Should I use this in production?
Probably not, but it's fun to play with. PR's to improve performance or stability are more than welcome!
