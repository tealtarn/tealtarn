# 📊 **TealTarn**

> **Diving Deep into Reasonably-Sized-Data**

> **BIG DATA was then. TealTarn is now.**  The era of massive cluster ETL is over—welcome to metadata-first, SQL-driven simplicity.

&#x20;   &#x20;

A minimal, SQL-first data transformation framework for teams migrating from monolithic clusters to agile lakehouse workflows. Built on **DuckDB** and **DuckLake** for a metadata-first experience that renders sprawling filesystems obsolete.

---

## ✨ Features

- 🎯 **SQL-First**: Pure SQL transformations—no Python scripting required.
- 🏠 **Zero Infrastructure**: Runs anywhere—laptop, VM, or in Marimo notebooks.
- 📱 **Portable**: Works on Windows, macOS, Linux, Docker, and cloud VMs.
- 📊 **Power BI Ready**: Outputs dimensional models that plug straight into BI tools.
- 🔍 **Metadata-Rich**: Embedded lineage and documentation in every table.
- 📝 **Git Friendly**: Version control your pipelines and SQL scripts.
- 🧠 **DuckLake Integration**: Manage your catalog & schema evolution without Avro sprawl.
- ⚡ **Named Pipelines**: Reusable, parameterized workflows for diverse workloads.

---

## 🚀 Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/tealtarn/tealtarn.git
cd tealtarn
```

**Linux/macOS**:

```bash
chmod +x install-linux.sh
./install-linux.sh
```

**Windows (PowerShell)**:

```powershell
# Run as Administrator
.\install-windows.ps1
```

### 2. Configure Connection

````bash
cp config/source_tables.yml config/source_tables_local.yml
# Edit your database details
```yaml
connection:
  type: mssql
  server: YOUR_SERVER
  database: SampleRetailDB
  trusted_connection: true

tables:
  - name: customers
    schema: dbo
    description: Customer master data
  - name: orders
    schema: dbo
    description: Order transactions
````

### 3. Run Pipelines in Marimo Notebooks

1. Open your favorite **Marimo** notebook.
2. `pip install tealtarn` or use the local dev install.
3. Use the built-in magic commands:
   ```sql
   %%tealtarn extract --all
   %%tealtarn run --pipeline retail_analytics
   ```

---

## 📁 Project Structure

```plaintext
parquet-pipelines/
├── 📂 config/                    # Configuration files
│   ├── source_tables.yml         # Database connections & table definitions
│   ├── pipeline.yml              # Main transformation pipeline
│   └── named_pipelines.yml       # Reusable, named pipelines
├── 📂 sql/                       # SQL transformation files
│   ├── 📂 silver/               # Data cleaning transformations
│   └── 📂 gold/                 # Business logic transformations
├── 📂 data/                      # Generated data (auto-created, gitignored)
│   ├── 📂 bronze/               # Raw extracted data
│   ├── 📂 silver/               # Cleaned data
│   └── 📂 gold/                 # Analytics-ready data
├── 📂 sample_data/               # Sample database setup
├── 📂 tests/                     # Test suite
├── 📄 ducklake_meta.duckdb      # DuckLake catalog metadata
└── 📄 README.md                 # This file
```

---

## 🎯 Use Cases

- Teams with strong SQL skills, minimal Python/DevOps.
- Migrating heavy stored-procedure stacks to modern analytics.
- Zero-config pipelines—from Marimo on your laptop to cloud VMs.
- Version-controlled, metadata-first lakehouse workflows.

---

## 🏗️ Architecture

### Medallion Layers

- 🥉 **Bronze**: Raw extraction
- 🥈 **Silver**: Cleaning & validation
- 🥇 **Gold**: Business-ready facts & dimensions

### DuckLake Highlights

- Queryable catalog & schema history
- Local file-based lakehouse (no Avro fragments)
- Standard `CREATE OR REPLACE TABLE` SQL

---

## 🐳 Docker & Marimo Support

```bash
# Start services (SQL Server & TealTarn)
docker-compose up -d
```

Then in Marimo:

```sql
%%tealtarn extract --all
%%tealtarn run --pipeline retail_analytics
```

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md).

---

© 2025 **TealTarn** · Built by Peter and friends

