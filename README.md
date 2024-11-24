# Database-Migration-Tool

**Database Migrator** is a Python script to migrate data between PostgreSQL and MySQL databases efficiently. It supports schema conversion and batch processing for large datasets.

## Features
- Convert table schemas between PostgreSQL and MySQL.
- Migrate data in batches for efficiency.
- Custom SQL query support for selective migrations.
- Verify migration by comparing record counts.

## Requirements
- Python 3.7 or higher
- Libraries: `sqlalchemy`, `pandas`, `tqdm`, `psycopg2`, `mysql-connector-python`

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/<your-username>/database-migrator.git
    cd database-migrator
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. Update the `source_db` and `target_db` connection strings in the script.
2. Run the script:
    ```bash
    python database_migrator.py
    ```
3. Migrate a table:
    ```python
    migrator.migrate_table("table_name", create_target=True)
    ```

## Example Connection Strings
- PostgreSQL: `postgresql://user:pass@localhost:5432/source_db`
- MySQL: `mysql://user:pass@localhost:3306/target_db`
