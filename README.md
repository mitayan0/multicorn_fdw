# multicorn_fdw

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/) 
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A collection of **Python Foreign Data Wrappers (FDWs)** for PostgreSQL using **Multicorn2**.  

- **servicenow_fdw** – Connect to ServiceNow Table API with full CRUD support.  
- **procg_fdw** – Generic REST API FDW with pagination, token authentication, and CRUD operations for our flask REST APIs.

---

## Features

- Dynamic python Foreign Data Wrappers for PostgreSQL
- Supports `SELECT`, `INSERT`, `UPDATE`, `DELETE`
- Generic REST API integration with authentication & pagination
- Supports `JSONB`, `TIMESTAMP` And other data type parsing

---

## Clone the repository

Open a terminal and run:

```bash
cd ~
git clone https://github.com/mitayan0/multicorn_fdw.git

sudo cp -r ~/multicorn_fdw /usr/local/lib/python3.10/dist-packages/