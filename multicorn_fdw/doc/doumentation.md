# Procg FDW Usage

## Create Server

```sql
CREATE SERVER procg_srv FOREIGN DATA WRAPPER multicorn
OPTIONS (wrapper 'multicorn_fdw.ApiFdw');

CREATE FOREIGN TABLE procg_tasks (
    id text,
    name text,
    status text
)
SERVER procg_srv
OPTIONS (
    url 'http://api.example.com/tasks',
    username 'user',
    password 'pass',
    primary_key 'id',
    pk_as_query_param 'true',
    page '1',
    limit '50',
    pagination_style 'path'
);

```

 ---

## Supports CRUD operations

```sql

-- SELECT
SELECT * FROM procg_tasks WHERE id = '123';

-- INSERT
INSERT INTO procg_tasks (id, name, status) VALUES ('124', 'Test Task', 'open');

-- UPDATE
UPDATE procg_tasks SET status = 'closed' WHERE id = '124';

-- DELETE
DELETE FROM procg_tasks WHERE id = '124';

```

---

## ServiceNow FDW

```sql
CREATE SERVER servicenow_srv FOREIGN DATA WRAPPER multicorn
OPTIONS (wrapper 'multicorn_fdw.ServiceNowFDW');

CREATE FOREIGN TABLE servicenow_incidents (
    sys_id text,
    number text,
    short_description text,
    priority text,
    state text
)
SERVER servicenow_srv
OPTIONS (
    api_url 'https://instance.service-now.com/api/now/table/incident',
    username 'admin',
    password 'secret',
    primary_key 'sys_id'
);

```

---
