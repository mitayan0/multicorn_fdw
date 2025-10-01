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

## Testing

This project uses `pytest` to validate both the `procg` and `servicenow` FDWs.

Prerequisites:

- Python 3.10+
- A virtual environment activated for this project

Install test dependencies:

```bash
pip install -U pytest python-dateutil
```

Run tests from the project root:

```bash
pytest -q
```

Optional coverage:

```bash
pip install pytest-cov
pytest --cov=multicorn_fdw -q
```

Notes:

- Tests mock all HTTP calls; no network is required.
- If you see `ImportError: No module named multicorn` or similar, ensure you are using the project's virtual environment and that `multicorn` is installed in that environment.
- On Windows PowerShell, activate the venv and run tests:

```powershell
# from project root
./venv/Scripts/Activate.ps1
python -V
python -c "import sys,site; print(sys.executable); print(site.getsitepackages())"
pytest -q
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
