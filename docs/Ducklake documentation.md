DuckLake Logo
⌘+k
0.1 (stable)

Documentation
Specification
DuckDB Extension
Introduction
Usage
Maintenance
Advanced Features
FAQ
Documentation / DuckDB Extension
Introduction
In DuckDB, DuckLake is supported through the ducklake extension.

Installation
Install the latest stable DuckDB. (The ducklake extensions requires DuckDB v1.3.0 "Ossivalis" or later.)

INSTALL ducklake;

Configuration
To use DuckLake, you need to make two decisions: which metadata catalog database you want to use and where you want to store those files. In the simplest case, you use a local DuckDB file for the metadata catalog and a local folder on your computer for file storage.

Creating a New Database
DuckLake databases are created by simply starting to use them with the ATTACH statement. In the simplest case, you can create a local, DuckDB-backed DuckLake like so:

ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;
USE my_ducklake;

This will create a file my_ducklake.ducklake, which is a DuckDB database with the DuckLake schema.

We also use USE so we don't have to prefix all table names with my_ducklake. Once data is inserted, this will also create a folder my_ducklake.ducklake.files in the same directory, where Parquet files are stored.

If you would like to use another directory, you can specify this in the DATA_PATH parameter for ATTACH:

ATTACH 'ducklake:my_other_ducklake.ducklake' AS my_other_ducklake (DATA_PATH 'some/other/path/');
USE ...;

The path is stored in the DuckLake metadata and does not have to be specified again to attach to an existing DuckLake catalog.

Attaching an Existing Database
Attaching to an existing database also uses the ATTACH syntax. For example, to re-connect to the example from the previous section in a new DuckDB session, we can just type:

ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;
USE my_ducklake;

Using DuckLake
DuckLake is used just like any other DuckDB database. You can create schemas and tables, insert data, update data, delete data, modify table schemas etc.

Note that – similarly to other data lake and lakehouse formats – the DuckLake format does not support indexes, primary keys, foreign keys, and UNIQUE or CHECK constraints.

Don't forget to either specify the database name of the DuckLake explicity or use USE. Otherwise you might inadvertently use the temporary, in-memory database.

Example
Let's observe what happens in DuckLake when we interact with a dataset. We will use the Netherlands train traffic dataset here.

We use the example DuckLake from above:

ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;
USE my_ducklake;

Let's now import the dataset into the a new table:

CREATE TABLE nl_train_stations AS
    FROM 'https://blobs.duckdb.org/nl_stations.csv';

Now Let's peek behind the courtains. The data was just read into a Parquet file, which we can also just query.

FROM glob('my_ducklake.ducklake.files/*');
FROM 'my_ducklake.ducklake.files/*.parquet' LIMIT 10;

But now lets change some things around. We're really unhappy with the name of the old name of the "Amsterdam Bijlmer ArenA" station now that the stadium has been renamed to "Johan Cruijff ArenA" and everyone here loves Johan. So let's change that.

UPDATE nl_train_stations SET name_long='Johan Cruijff ArenA' WHERE code = 'ASB';

Poof, its changed. We can confirm:

SELECT name_long FROM nl_train_stations WHERE code = 'ASB';

In the background, more files have appeared:

FROM glob('my_ducklake.ducklake.files/*');

We now see three files. The original data file, the rows that were deleted, and the rows that were inserted. Like most systems, DuckLake models updates as deletes followed by inserts. The deletes are just a Parquet file, we can query it:

FROM 'my_ducklake.ducklake.files/ducklake-*-delete.parquet';

The file should contain a single row that marks row 29 as deleted. A new file has appared that contains the new values for this row.

There are now three snapshots, the table creation, data insertion, and the update. We can query that using the snapshots() function:

FROM my_ducklake.snapshots();

And we can query this table at each point:

SELECT name_long FROM nl_train_stations AT (VERSION => 1) WHERE code = 'ASB';
SELECT name_long FROM nl_train_stations AT (VERSION => 2) WHERE code = 'ASB';

Time travel finally achieved!

Detaching from a DuckLake
To detach from a DuckLake, make sure that your DuckLake is not your default database, then use the DETACH statement:

USE memory;
DETACH my_ducklake;

Using DuckLake from a Client
DuckDB works with any DuckDB client that supports DuckDB version 1.3.0.

In this article
Installation
Configuration
Creating a New Database
Attaching an Existing Database
Using DuckLake
Example
Detaching from a DuckLake
Using DuckLake from a Client




\\\\\\\
DuckLake Logo
⌘+k
0.1 (stable)

Documentation
Specification
DuckDB Extension
Introduction
Usage
Choosing a Catalog Database
Choosing Storage
Snapshots
Schema Evolution
Time Travel
Maintenance
Advanced Features
FAQ
Documentation / DuckDB Extension / Usage
Choosing a Catalog Database
You may choose different catalog databases for your DuckLake. The choice depends on several factors, including whether you need to use multiple clients, which database systems are available in your organization, etc.

On the technical side, consider the following:

If you would like to perform local data warehousing with a single client, use DuckDB as the catalog database.
If you would like to perform local data warehousing using multiple local clients, use SQLite as the catalog database.
If you would like to operate a multi-user lakehouse with potentially remote clients, choose a transactional client-server database system as the catalog database: MySQL or PostgreSQL.
DuckDB
DuckDB can, of course, natively connect to DuckDB database files. So, to get started, you only need to install the ducklake extension and attach to your DuckLake:

INSTALL ducklake;

ATTACH 'ducklake:metadata.ducklake' AS my_ducklake;
USE my_ducklake;

Note that if you are using DuckDB as your catalog database, you're limited to a single client.

PostgreSQL
DuckDB can interact with a PostgreSQL database using the postgres extension. Install the ducklake and the postgres extension, and attach to your DuckLake as follows:

INSTALL ducklake;
INSTALL postgres;

-- Make sure that the database `ducklake_catalog` exists in PostgreSQL.
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost' AS my_ducklake
    (DATA_PATH 'data_files/');
USE my_ducklake;

For details on how to configure the connection, see the postgres extension's documentation.

The ducklake and postgresql extensions require PostgreSQL 12 or newer.

SQLite
DuckDB can read and write a SQLite database file using the sqlite extension. Install the ducklake and the sqlite extension, and attach to your DuckLake as follows:

INSTALL ducklake;
INSTALL sqlite;

ATTACH 'ducklake:sqlite:metadata.sqlite' AS my_ducklake
    (DATA_PATH 'data_files/');
USE my_ducklake;

While SQLite doesn't allow concurrent reads and writes, its default mode is to ATTACH and DETACH for every query, together with providing a “retry time-out” for queries when a write-lock is encountered. This allows a reasonable amount of multi-processing support (effectively hiding the single-writer model).

MySQL
DuckDB can interact with a MySQL database using the mysql extension. Install the ducklake and the mysql extension, and attach to your DuckLake as follows:

INSTALL ducklake;
INSTALL mysql;

-- Make sure that the database `ducklake_catalog` exists in MySQL
ATTACH 'ducklake:mysql:db=ducklake_catalog host=localhost' AS my_ducklake
    (DATA_PATH 'data_files/');
USE my_ducklake;

For details on how to configure the connection, see the mysql extension's documentation.

Using the ducklake and mysql extensions require MySQL 8 or newer.

In this article
DuckDB
PostgreSQL
SQLite
MySQL







\\\\\\\

DuckLake Logo
⌘+k
0.1 (stable)

Documentation
Specification
DuckDB Extension
Introduction
Usage
Choosing a Catalog Database
Choosing Storage
Snapshots
Schema Evolution
Time Travel
Maintenance
Advanced Features
FAQ
Documentation / DuckDB Extension / Usage
Choosing Storage
DuckLake as a concept will never change existing files, neither by changing existing content nor by appending to existing files. This greatly reduces the consistency requirements of file systems and greatly simplifies caching.

The DuckDB ducklake extension can work with any file system backend that DuckDB supports. This currently includes:

local files and folders
cloud object store like
AWS S3 and compatible (e.g. CloudFlare R2, Hetzner Object Storage, etc.)
Google Cloud Storage
Azure Blob Store
virtual network attached file systems
NFS
SMB
FUSE
Python fsspec file systems …
When choosing storage, its important to consider the following factors

access latency and data transfer throughput, a cloud further away will be accessible to everyone but have a higher latency. local files are very fast, but not accessible to anyone else. A compromise might be a site-local storage server.
scalability and cost, an object store is quite* scalable, but potentially charges for data transfer. A local server might not incur significant operating expenses, but might struggle serving thousands of clients.
It might also be interesting to use DuckLake encryption when choosing external cloud storage.



\\\\\

DuckLake Logo
⌘+k
0.1 (stable)

Documentation
Specification
DuckDB Extension
Introduction
Usage
Choosing a Catalog Database
Choosing Storage
Snapshots
Schema Evolution
Time Travel
Maintenance
Advanced Features
FAQ
Documentation / DuckDB Extension / Usage
Snapshots
Snapshots represent commits made to DuckLake. Every snapshot performs a set of changes that alter the state of the database. Snapshots can create tables, insert or delete data, and alter schemas.

Changes can only be made to DuckLake using snapshots. Every set of changes must be accompanied by a snapshot.

Listing Snapshots
The set of snapshots can be queried using the snapshots function. This returns a list of all snapshots and their changesets.

ATTACH 'ducklake:snapshot_test.db' AS snapshot_test;
SELECT * FROM snapshot_test.snapshots();

snapshot_id	snapshot_time	schema_version	changes
0	2025-05-26 17:03:37.746+00	0	{schemas_created=[main]}
1	2025-05-26 17:03:38.66+00	1	{tables_created=[main.tbl]}
2	2025-05-26 17:03:38.748+00	1	{tables_inserted_into=[1]}
3	2025-05-26 17:03:39.788+00	1	{tables_deleted_from=[1]}
In this article
Listing Snapshots


\\\\\\\


DuckLake Logo
⌘+k
0.1 (stable)

Documentation
Specification
DuckDB Extension
Introduction
Usage
Choosing a Catalog Database
Choosing Storage
Snapshots
Schema Evolution
Time Travel
Maintenance
Advanced Features
FAQ
Documentation / DuckDB Extension / Usage
Schema Evolution
DuckLake supports the evolution of the schemas of tables without requiring any data files to be rewritten. The schema of a table can be changed using the ALTER TABLE statement. The following statements are supported:

Adding Columns / Fields
-- add a new column of type INTEGER, with default value NULL
ALTER TABLE tbl ADD COLUMN new_column INTEGER;
-- add a new column with an explicit default value
ALTER TABLE tbl ADD COLUMN new_column VARCHAR DEFAULT 'my_default';

Fields can be added to columns of type struct. The path to the struct column must be specified, followed by the name of the new field and the type of the new field.

-- add a new field of type INTEGER, with default value NULL
ALTER TABLE tbl ADD COLUMN nested_column.new_field INTEGER;

Dropping Columns / Fields
-- drop the top-level column `new_column` from the table
ALTER TABLE tbl DROP COLUMN new_column;

Fields can be dropped by specifying the full path to the field.

-- drop the field `new_field` from the struct column `nested_column`
ALTER TABLE tbl DROP COLUMN nested_column.new_field;

Renaming Columns / Fields
-- rename the top-level column "new_column" to "new_name"
ALTER TABLE tbl RENAME new_column TO new_name;

Field scan be renamed by specifying the full path to the field.

-- rename the field "new_field" within the struct column "nested_column" to "new_name"
ALTER TABLE tbl RENAME nested_column.new_field TO new_name;

Type Promotion
The types of columns can be changed.

-- change the type of col1 to BIGINT
ALTER TABLE tbl ALTER col1 SET TYPE BIGINT;
-- change the type of field "new_field" within the struct column "nested_column" to BIGINT
ALTER TABLE tbl ALTER nested_column.new_field SET TYPE BIGINT;

Note that not all type changes are valid. Only type promotions are supported. Type promotions must be lossless. As such, valid type promotions are promoting from a narrower type (int32) to a wider type (int64).

The full set of valid type promotions is as follows:

Source	Target
int8	int16, int32, int64
int16	int32, int64
int32	int64
uint8	uint16, uint32, uint64
uint16	uint32, uint64
uint32	uint64
float32	float64
Field Identifiers
Columns are tracked using field identifiers. These identifiers are stored in the column_id field of the ducklake_column table. The identifiers are also written to each of the data files. For Parquet files, these are written in the field_id field. These identifiers are used to reconstruct the data of a table for a given snapshot.

When reading the data for a table, the schema together with the correct field identifiers is read from the ducklake_column table. Data files can contain any number of columns that exist in that schema, and can also contain columns that do not exist in that schema.

If we drop a column, previously written data files still contain the dropped column.
If we add a column, previously written data files do not contain the new column.
If we change the type of a column, previously written data files contain data for the column in the old type.
To reconstruct the correct table data for a given snapshot, we must perform field id remapping. This is done as follows:

Data for a column is read from the column with the corresponding field_id. The data types might not match in case of type promotion. In this case, the values must be cast to the correct type of the column.
Any column that has a field_id that exists in the data file but not in the table schema must be ignored
Any column that has a field_id that does not exist in the data file must be replaced with the initial_default value in the ducklake_column table
In this article
Adding Columns / Fields
Dropping Columns / Fields
Renaming Columns / Fields
Type Promotion
Field Identifiers


\\\\\\


DuckLake Logo
⌘+k
0.1 (stable)

Documentation
Specification
DuckDB Extension
Introduction
Usage
Choosing a Catalog Database
Choosing Storage
Snapshots
Schema Evolution
Time Travel
Maintenance
Advanced Features
FAQ
Documentation / DuckDB Extension / Usage
Time Travel
In DuckLake, every snapshot represents a consistent state of the database. DuckLake keeps a record of all historic snapshots and their changesets, unless compaction is triggered and historic snapshots are explicitly deleted.

Using time travel, it is possible to query the state of the database as of any recorded snapshot. The snapshot to query can be specified either (1) using a timestamp, or (2) explicitly using a snapshot identifier. The snapshots function can be used to obtain a list of valid snapshots for a given DuckLake database.

Examples
Query the table at a specific snapshot version.

SELECT * FROM tbl AT (VERSION => 3);

Query the table as it was last week.

SELECT * FROM tbl AT (TIMESTAMP => now() - INTERVAL '1 week');

Attach a DuckLake database at a specific snapshot version.

ATTACH 'ducklake:file.db' (SNAPSHOT_VERSION 3);

Attach a DuckLake database as it was at a specific time.

ATTACH 'ducklake:file.db' (SNAPSHOT_TIME '2025-05-26 00:00:00');






\\\\\\


DuckLake Logo
⌘+k
0.1 (stable)

Documentation
Specification
DuckDB Extension
Introduction
Usage
Choosing a Catalog Database
Choosing Storage
Snapshots
Schema Evolution
Time Travel
Maintenance
Advanced Features
FAQ
Documentation / DuckDB Extension / Usage
Time Travel
In DuckLake, every snapshot represents a consistent state of the database. DuckLake keeps a record of all historic snapshots and their changesets, unless compaction is triggered and historic snapshots are explicitly deleted.

Using time travel, it is possible to query the state of the database as of any recorded snapshot. The snapshot to query can be specified either (1) using a timestamp, or (2) explicitly using a snapshot identifier. The snapshots function can be used to obtain a list of valid snapshots for a given DuckLake database.

Examples
Query the table at a specific snapshot version.

SELECT * FROM tbl AT (VERSION => 3);

Query the table as it was last week.

SELECT * FROM tbl AT (TIMESTAMP => now() - INTERVAL '1 week');

Attach a DuckLake database at a specific snapshot version.

ATTACH 'ducklake:file.db' (SNAPSHOT_VERSION 3);

Attach a DuckLake database as it was at a specific time.

ATTACH 'ducklake:file.db' (SNAPSHOT_TIME '2025-05-26 00:00:00');






\\\\\\



DuckLake Logo
⌘+k
0.1 (stable)

Documentation
Specification
Introduction
Data Types
Queries
Tables
DuckDB Extension
Introduction
Usage
Maintenance
Advanced Features
FAQ
Documentation / Specification
Queries
Reading Data
DuckLake specifies tables and update transactions to modify them. DuckLake is not a black box, all metadata is stored as SQL tables under the user's control. Of course, they can be queried in whichever way is best for a client. Below we describe a small working example to retrieve table data.

The information below is to provide transparency to users and to aid developers making their own implementation of DuckLake. The ducklake DuckDB extension is able to execute those operations in the background.

Get Current Snapshot
Before anything else we need to find a snapshot ID to be queries. There can be many snapshots in the ducklake_snapshot table. A snapshot ID is a continously increasing number that identifies a snapshot. In most cases, you would query the most recent one like so:

SELECT snapshot_id FROM ducklake_snapshot
WHERE snapshot_id =
    (SELECT max(snapshot_id) FROM ducklake_snapshot);

List Schemas
A DuckLake catalog can contain many SQL-style schemas, which each can contain many tables. These are listed in the ducklake_schema table. Here's how we get the list of valid schemas for a given snapshot:

SELECT schema_id, schema_name
FROM ducklake_schema
WHERE
    SNAPSHOT_ID >= begin_snapshot AND
    (SNAPSHOT_ID < end_snapshot OR end_snapshot IS NULL);

where

SNAPSHOT_ID is a BIGINT referring to the snapshot_id column in the ducklake_snapshot table
List Tables
We can list the tables available in a schema for a specific snapshot using the ducklake_table table:

SELECT table_id, table_name
FROM ducklake_table
WHERE
    schema_id = SCHEMA_ID AND
    SNAPSHOT_ID >= begin_snapshot AND
    (SNAPSHOT_ID < end_snapshot OR end_snapshot IS NULL);

where

SCHEMA_ID is a BIGINT referring to the schema_id column in the ducklake_schema table
SNAPSHOT_ID is a BIGINT referring to the snapshot_id column in the ducklake_snapshot table
Show the Structure of a Table
For each given table, we can list the available top-level columns using the ducklake_column table:

SELECT column_id, column_name, column_type
FROM ducklake_column
WHERE
    table_id = TABLE_ID AND
    parent_column IS NULL AND
    SNAPSHOT_ID >= begin_snapshot AND
    (SNAPSHOT_ID < end_snapshot OR end_snapshot IS NULL)
ORDER BY column_order;

where

TABLE_ID is a BIGINT referring to the table_id column in the ducklake_table table
SNAPSHOT_ID is a BIGINT referring to the snapshot_id column in the ducklake_snapshot table
Note
that DuckLake supports nested columns – the filter for parent_column IS NULL only shows the top-level columns.

For the list of supported data types, please refer to the “Data Types” page.

SELECT
Now that we know the table structure we can query actual data from the Parquet files that store table data. We need to join the list of data files with the list of delete files (if any). There can be at most one delete file per file in a single snapshot.

SELECT data.path AS data_file_path, del.path AS delete_file_path
FROM ducklake_data_file AS data
LEFT JOIN (
    SELECT *
    FROM ducklake_delete_file
    WHERE
        SNAPSHOT_ID >= begin_snapshot AND
        (SNAPSHOT_ID < end_snapshot OR end_snapshot IS NULL)
    ) AS del
USING (data_file_id)
WHERE
    data.table_id = TABLE_ID AND
    SNAPSHOT_ID >= data.begin_snapshot AND
    (SNAPSHOT_ID < data.end_snapshot OR data.end_snapshot IS NULL)
ORDER BY file_order;

where (again)

TABLE_ID is a BIGINT referring to the table_id column in the ducklake_table table
SNAPSHOT_ID is a BIGINT referring to the snapshot_id column in the ducklake_snapshot table
Now we have a list of files. In order to reconstruct actual table rows, we need to read all rows from the data_file_path files and remove the rows labeled as deleted in the delete_file_path.

Not all files have to contain all the columns currently defined in the table, some files may also have columns that existed previously but have been removed.

DuckLake also supports changing the schema, see schema evolution.

In DuckLake, paths can be relative to the initially specified data path. Whether path is relative or not is stored in the ducklake_data_file and ducklake_delete_file entries (path_is_relative) to the data_path prefix from ducklake_metadata.

SELECT with File Pruning
One of the main strengths of Lakehouse formats is the ability to prune files that cannot contain data relevant to the query. The ducklake_file_column_statistics table contains the file-level statistics. We can use the information there to prune the list of files to be read if a filter predicate is given.

We can get a list of all files that are part of a given table like described above. We can then reduce that list to only relevant files by querying the per-file column statistics. For example, for scalar equality we can find the relevant files using the query below:

SELECT data_file_id
FROM ducklake_file_column_statistics
WHERE
    table_id  = TABLE_ID AND
    column_id = COLUMN_ID AND
    (SCALAR >= min_value OR min_value IS NULL) AND
    (SCALAR <= max_value OR max_value IS NULL);

where (again)

TABLE_ID is a BIGINT referring to the table_id column in the ducklake_table table.
COLUMN_ID is a BIGINT referring to the column_id column in the ducklake_column table.
SCALAR is the scalar comparision value for the pruning.
Of course, other filter predicates like greater than etc. will require slighlty different filtering here.

The minimum and maximum values for each column are stored as strings and need to be cast for correct range filters on numeric columns.

Writing Data
Snapshot Creation
Any changes to data stored in DuckLake require the creation of a new snapshot. We need to:

create a new snapshot in ducklake_snapshot and
log the changes a snapshot made in ducklake_snapshot_changes
INSERT INTO ducklake_snapshot (
    snapshot_id,
    snapshot_timestamp,
    schema_version,
    next_catalog_id,
    next_file_id
)
VALUES (
    SNAPSHOT_ID,
    now(),
    SCHEMA_VERSION,
    NEXT_CATALOG_ID,
    NEXT_FILE_ID
);

INSERT INTO ducklake_snapshot_changes (
    snapshot_id,
    snapshot_changes
)
VALUES (
    SNAPSHOT_ID,
    CHANGES
);

where

SNAPSHOT_ID is the new snapshot identifier. This should be max(snapshot_id) + 1.
SCHEMA_VERSION is the schema version for the new snapshot. If any schema changes are made, this needs to be incremented. Otherwise the previous snapshot's schema_version can be re-used.
NEXT_CATALOG_ID gives the next unused identifier for tables, schemas, or views. This only has to be incremented if new catalog entries are created.
NEXT_FILE_ID is the same but for data or delete files.
CHANGES contains a list of changes performed by the snapshot. See the list of possible values in the ducklake_snapshot_changes table's documentation.
CREATE SCHEMA
A schema is a collection of tables. In order to create a new schema, we can just insert into the ducklake_schema table:

INSERT INTO ducklake_schema (
    schema_id,
    schema_uuid,
    begin_snapshot,
    end_snapshot,
    schema_name
)
VALUES (
    SCHEMA_ID,
    uuid(),
    SNAPSHOT_ID,
    NULL,
    SCHEMA_NAME
);

where

SCHEMA_ID is the new schema identifier. This should be created by incrementing next_catalog_id from the previous snapshot.
SNAPSHOT_ID is the snapshot identifier of the new snapshot as described above.
SCHEMA_NAME is just the name of the new schema.
CREATE TABLE
Creating a table in a schema is very similar to creating a schema. We insert into the ducklake_table table:

INSERT INTO ducklake_table (
    table_id,
    table_uuid,
    begin_snapshot,
    end_snapshot,
    schema_id,
    table_name
)
VALUES (
    TABLE_ID,
    uuid(),
    SNAPSHOT_ID,
    NULL,
    SCHEMA_ID,
    TABLE_NAME
);

where

TABLE_ID is the new table identifier. This should be created by further incrementing next_catalog_id from the previous snapshot.
SNAPSHOT_ID is the snapshot identifier of the new snapshot as described above.
SCHEMA_ID is a BIGINT referring to the schema_id column in the ducklake_schema table table.
TABLE_NAME is just the name of the new table.
A table needs some columns, we can add columns to the new table by inserting into the ducklake_column table table. For each column to be added, we run the following query:

INSERT INTO ducklake_column (column_id,
    begin_snapshot,
    end_snapshot,
    table_id,
    column_order,
    column_name,
    column_type,
    nulls_allowed
)
VALUES (
    COLUMN_ID,
    SNAPSHOT_ID,
    NULL,
    TABLE_ID,
    COLUMN_ORDER,
    COLUMN_NAME,
    COLUMN_TYPE,
    NULLS_ALLOWED
);

where

COLUMN_ID is the new column identifier. This ID must be unique within the table over its entire life time.
SNAPSHOT_ID is the snapshot identifier of the new snapshot as described above.
TABLE_ID is a BIGINT referring to the table_id column in the ducklake_table table.
COLUMN_ORDER is a number that defines where the column is placed in an ordered list of columns.
COLUMN_NAME is just the name of the column.
COLUMN_TYPE is the data type of the column. See the “Data Types” page for details.
NULLS_ALLOWED is a boolean that defines if NULL values can be stored in the column. Typically set to true.
We skipped some complexity in this example around default values and nested types and just left those fields as NULL. See the table schema definition for additional details.

INSERT
Inserting data into a DuckLake table consists of two main steps: first, we need to write a Parquet file containing the actual row data to storage, and second, we need to register that file in the metadata tables and update global statistics. Let's assume the file has already been written.

INSERT INTO ducklake_data_file (
    data_file_id,
    table_id,
    begin_snapshot,
    end_snapshot,
    path,
    path_is_relative,
    file_format,
    record_count,
    file_size_bytes,
    footer_size,
    row_id_start
)
VALUES (
    DATA_FILE_ID,
    TABLE_ID,
    SNAPSHOT_ID,
    NULL,
    PATH,
    true,
    'parquet',
    RECORD_COUNT,
    FILE_SIZE_BYTES,
    FOOTER_SIZE,
    ROW_ID_START
);

where

DATA_FILE_ID is the new data file identifier. This ID must be unique within the table over its entire life time.
TABLE_ID is a BIGINT referring to the table_id column in the ducklake_table table.
SNAPSHOT_ID is the snapshot identifier of the new snapshot as described above.
PATH is the file name relative to the DuckLake data path from the top-level metadata.
RECORD_COUNT is the number of rows in the file.
FILE_SIZE_BYTES is the file size.
FOOTER_SIZE is the position of the Parquet footer. This helps with efficiently reading the file.
ROW_ID_START is the first logical row ID from the file. This number can be read from the ducklake_table_stats table via column next_row_id.
We have omitted some complexity around relative paths, encrypted files, partitioning and partial files in this example. Refer to the ducklake_data_file table documentation for details.

DuckLake also supports changing the schema, see schema evolution.

We will also have to update some statistics in the ducklake_table_stats table and ducklake_table_column_stats table` tables.

UPDATE ducklake_table_stats SET
    record_count = record_count + RECORD_COUNT,
    next_row_id = next_row_id + RECORD_COUNT,
    file_size_bytes = file_size_bytes + FILE_SIZE_BYTES
WHERE table_id = TABLE_ID;

UPDATE ducklake_table_column_stats
SET
    contains_null = contains_null OR NULL_COUNT > 0,
    contains_nan = contains_nan OR NAN_COUNT > 0,
    min_value = min(min_value, MIN_VALUE),
    max_value = max(max_value, MAX_VALUE)
WHERE
    table_id  = TABLE_ID AND
    column_id = COLUMN_ID;

INSERT INTO ducklake_file_column_statistics (
    data_file_id,
    table_id,
    column_id,
    value_count,
    null_count,
    nan_count,
    min_value,
    max_value,
    contains_nan
)
VALUES (
    DATA_FILE_ID,
    TABLE_ID,
    COLUMN_ID,
    RECORD_COUNT,
    NULL_COUNT,
    NAN_COUNT,
    MIN_VALUE,
    MAX_VALUE,
    NAN_COUNT > 0;
);

where

TABLE_ID is a BIGINT referring to the table_id column in the ducklake_table table.
COLUMN_ID is a BIGINT referring to the column_id column in the ducklake_column table.
DATA_FILE_ID is a BIGINT referring to the data_file_id column in the ducklake_data_file table.
RECORD_COUNT is the number of values (including NULL and NaN values) in the file column.
NULL_COUNT is the number of NULL values in the file column.
NAN_COUNT is the number of NaN values in the file column (floating-point only).
MIN_VALUE is the minimum value in the file column as a string.
MAX_VALUE is the maximum value in the file column as a string.
FILE_SIZE_BYTES is the size of the new Parquet file.
This example assumes there are already rows in the table. If there are none, we need to use INSERT instead here. We also skipped the column_size_bytes column here, it can safely be set to NULL.

In this article
Reading Data
Get Current Snapshot
List Schemas
List Tables
Show the Structure of a Table
SELECT
SELECT with File Pruning
Writing Data
Snapshot Creation
CREATE SCHEMA
CREATE TABLE
INSERT



\\\\\

DuckLake Logo
Documentation
Resources
GitHub

Frequently Asked Questions
Overview
Why should I use DuckLake?
DuckLake provides a lightweight one-stop solution if you need a data lake and catalog.

You can use DuckLake for a “multiplayer DuckDB” setup with multiple DuckDB instances reading and writing the same dataset – a concurrency model not supported by vanilla DuckDB.

If you only use DuckDB for both your DuckLake entry point and your catalog database, you can still benefit from DuckLake: you can run time travel queries, exploit data partitioning, and can store your data in multiple files instead of using a single (potentially very large) database file.

Is DuckLake an open table format?
DuckLake includes an open table format but it's also a data lakehouse format, meaning that it also contains a catalog to encode the schema of the data stored. When comparing to other technologies, DuckLake is similar to Delta Lake with Unity Catalog and Iceberg with Lakekeeper or Polaris.

What is “DuckLake”?
First of all, a catchy name for a DuckDB-originated technology for data lakes and lakehouses. More seriously, the term “DuckLake” can refer to three things:

the specification of the DuckLake lakehouse format,
the ducklake DuckDB extension, which supports reading/writing datasets in the DuckLake specification,
a DuckLake, a dataset stored using the DuckLake lakehouse format.
Where can I download the DuckLake logo?
You can download the logo package. You can also download individual logos:

Dark mode, inline layout: png, svg
Dark mode, stacked layout: png, svg
Light mode, inline layout: png, svg
Light mode, stacked layout: png, svg
Architecture
What are the main components of DuckLake?
DuckLake needs a storage layer (both blob storage and block-based storage work) and a catalog database (any SQL-compatible database works).

Does DuckLake work on AWS S3 (or a compatible storage)?
DuckLake can store the data files (Parquet files) on the AWS S3 blob storage or compatible solutions such as Azure Blob Storage, Google Cloud Storage or Cloudflare R2. You can run the catalog database anywhere, e.g., in an AWS Aurora database.

DuckLake in Operation
Is DuckLake production-ready?
While we tested DuckLake extensively, it is not yet production-ready as demonstrated by its version number . We expect DuckLake to mature over the course of 2025.

How is authentication implemented in DuckLake?
DuckLake piggybacks on the authentication of the metadata catalog database. For example, if your catalog database is Postgres, you can use Postgres' authentication and authorization methods to protect your DuckLake. This is particularly effective when enabling encryption of DuckLake files.

How does DuckLake deal with the “small files problem”?
The “small files problem” is a well-known problem in data lake formats and occurs e.g. when data is inserted in small batches, yielding many small files with each storing only a small amount of data. DuckLake significantly mitigates this problem by storing the metadata in a database system (catalog database) and making the compaction step simple. DuckLake also harnesses the catalog database to stage data (a technique called “data inlining”) before serializing it into Parquet files. Further improvements are on the roadmap.

Features
Are constraints such as primary keys and foreign keys supported?
No. Similarly to other data lakehouse technologies, DuckLake does not support constraints, keys, or indexes.

Can I export my DuckLake into other lakehouse formats?
This is currently not supported, but planned for the future. Currently, you can export DuckLake into a DuckDB database and export it into e.g. vanilla Parquet files.

Are DuckDB database files supported as the data files for DuckLake?
The data files of DuckLake must be stored in Parquet. Using DuckDB files as storage are not supported at the moment.

Are there any practical limits to the size of data and the number of snapshots?
No. The only limitation is the catalog database's performance but even with a relatively slow catalog database, you can have terabytes of data and millions of snapshots.

Development
How is DuckLake tested?
DuckLake receives extensive testing, including running the applicable subset of DuckDB's thorough test suite. That said, if you encounter any problems using DuckLake, please submit an issue in the DuckLake issue tracker.

How can I contribute to DuckLake?
If you encounter any problems using DuckLake, please submit an issue in the DuckLake issue tracker. If you have any suggestions or feature requests, please open a ticket in DuckLake's discussion forum. You are also welcome to implement support in other systems for DuckLake following the specification.

What is the license of DuckLake?
The DuckLake specification and the DuckLake DuckDB extension are released under the MIT license.

Documentation
Manifesto
Specification
DuckLake DuckDB extension
DuckDB
DuckDB home
DuckDB installation
Development
Issues
Discussions
© 2025 DuckDB Foundation



DuckLake Logo
Documentation
Resources
GitHub

The DuckLake Manifesto:
SQL as a Lakehouse Format
Authors: Mark Raasveldt and Hannes Mühleisen

DuckLake simplifies lakehouses by using a standard SQL database for all metadata, instead of complex file-based systems, while still storing data in open formats like Parquet. This makes it more reliable, faster, and easier to manage.

Would you rather listen to the content of this manifesto? We also released a podcast episode explaining how we came up with the DuckLake format.

Background
Innovative data systems like BigQuery and Snowflake have shown that disconnecting storage and compute is a great idea in a time where storage is a virtualized commodity. That way, both storage and compute can scale independently and we don't have to buy expensive database machines just to store tables we will never read.

At the same time, market forces have pushed people to insist that data systems use open formats like Parquet to avoid the all-too-common hostage taking of data by a single vendor. In this new world, lots of data systems happily frolic around a pristine “data lake” built on Parquet and S3 and all was well. Who needs those old school databases anyway!

But quickly it emerged that – shockingly – people would like to make changes to their dataset. Simple appends worked pretty well by just dropping more files into a folder, but anything beyond that required complex and error-prone custom scripts without any notion of correctness or – Codd beware – transactional guarantees.

An actual lakehouse
An actual lakehouse. Maybe more like a cabin on a lake.

Iceberg and Delta
To address the basic task of changing data in the lake, various new open standards emerged, most prominently Apache Iceberg and Linux Foundation Delta Lake. Both formats were designed to essentially recover some sanity of making changes to tables without giving up the basic premise: use open formats on blob storage. For example, Iceberg uses a maze of JSON and Avro files to define schemas, snapshots and which Parquet files are part of the table at a point in time. The result was christened the “Lakehouse”, effectively an addition of database features to data lakes that enabled a lot of new exciting use cases for data management, e.g., cross-engine data sharing.

Iceberg table architecture Iceberg table architecture

But both formats hit a snag in the road: finding the latest version of a table is tricky in blob stores with their mercurial consistency guarantees. It’s tricky to atomically (the “A” in ACID) swap a pointer to make sure everyone sees the latest version. Iceberg and Delta Lake also only really know about a single table, but people – again, shockingly – wanted to manage multiple tables.

Catalogs
The solution was another layer of technology: we added a catalog service on top of the various files. That catalog service in turn talks to a database that manages all the table folder names. It also manages the saddest table of all time that only contains a single row for each table with the current version number. We can now borrow the database’s transactional guarantees around updating that number and everyone’s happy.

Iceberg catalog architecture Iceberg catalog architecture

A Database You Say?
But here’s the problem: Iceberg and Delta Lake were specifically designed to not require a database. Their designers went to great lengths to encode all information needed to efficiently read and update tables into files on the blob store. They make many compromises to achieve this. For example, every single root file in Iceberg contains all existing snapshots complete with schema information, etc. For every single change, a new file is written that contains the complete history. A lot of other metadata had to be batched together, e.g., in the two-layer manifest files to avoid writing or reading too many small files, something that would not be efficient on blob stores. Making small changes to data is also a largely unsolved problem that requires complex cleanup procedures that are still not very well understood nor supported by open-source implementations. Entire companies exist and are still being started to solve this problem of managing fast-changing data. Almost as if a specialized data management system of sorts would be a good idea.

But as pointed out above, the Iceberg and Delta Lake designs already had to compromise and add a database as part of the catalog for consistency. However, they never revisited the rest of their design constraints and tech stack to adjust for this fundamental design change.

DuckLake
Here at DuckDB, we actually like databases. They are amazing tools to safely and efficiently manage fairly large datasets. Once a database has entered the Lakehouse stack anyway, it makes an insane amount of sense to also use it for managing the rest of the table metadata! We can still take advantage of the “endless” capacity and “infinite” scalability of blob stores for storing the actual table data in open formats like Parquet, but we can much more efficiently and effectively manage the metadata needed to support changes in a database! Coincidentally, this is also what Google BigQuery (with Spanner) and Snowflake (with FoundationDB) have chosen, just without the open formats at the bottom.

DuckLake's architecture DuckLake's architecture: Just a database and some Parquet files

To resolve the fundamental problems of the existing Lakehouse architecture, we have created a new open table format called DuckLake. DuckLake re-imagines what a “Lakehouse” format should look like by acknowledging two simple truths:

Storing data files in open formats on blob storage is a great idea for scalability and to prevent lock-in.
Managing metadata is a complex and interconnected data management task best left to a database management system.
The basic design of DuckLake is to move all metadata structures into a SQL database, both for catalog and table data. The format is defined as a set of relational tables and pure-SQL transactions on them that describe data operations like schema creation, modification, and addition, deletion and updating of data. The DuckLake format can manage an arbitrary number of tables with cross-table transactions. It also supports “advanced” database concepts like views, nested types, transactional schema changes etc.; see below for a list. One major advantage of this design is by leveraging referential consistency (the “C” in ACID), the schema makes sure there are e.g. no duplicate snapshot ids.

DuckLake schema DuckLake schema

Which exact SQL database to use is up to the user, the only requirements are that the system supports ACID operations and primary keys along with standard SQL support. The DuckLake-internal table schema is intentionally kept simple in order to maximize compatibility with different SQL databases. Here is the core schema through an example.

Let's follow the sequence of queries that occur in DuckLake when running the following query on a new, empty table:

INSERT INTO demo VALUES (42), (43);

BEGIN TRANSACTION;
  -- some metadata reads skipped here
  INSERT INTO ducklake_data_file VALUES (0, 1, 2, NULL, NULL, 'data_files/ducklake-8196...13a.parquet', 'parquet', 2, 279, 164, 0, NULL, NULL);
  INSERT INTO ducklake_table_stats VALUES (1, 2, 2, 279);
  INSERT INTO ducklake_table_column_stats VALUES (1, 1, false, NULL, '42', '43');
  INSERT INTO ducklake_file_column_statistics VALUES (0, 1, 1, NULL, 2, 0, 56, '42', '43', NULL)
  INSERT INTO ducklake_snapshot VALUES (2, now(), 1, 2, 1);
  INSERT INTO ducklake_snapshot_changes VALUES (2, 'inserted_into_table:1');
COMMIT;

We see a single coherent SQL transaction that:

Inserts the new Parquet file path
Updates the global table statistics (now has more rows)
Updates the global column statistics (now has a different minimum and maximum value)
Updates the file column statistics (also record min/max among other things)
Creates a new schema snapshot (#2)
Logs the changes that happened in the snapshot
Note that the actual write to Parquet is not part of this sequence, it happens beforehand. But no matter how many values are added, this sequence has the same (low) cost.

Let's discuss the three principles of DuckLake: Simplicity, Scalability and Speed.

Simplicity
DuckLake follows the DuckDB design principles of keeping things simple and incremental. In order to run DuckLake on a laptop, it is enough to just install DuckDB with the ducklake extension. This is great for testing purposes, development and prototyping. In this case, the catalog store is just a local DuckDB file.

The next step is making use of external storage systems. DuckLake data files are immutable, it never requires modifying files in place or re-using file names. This allows use with almost any storage system. DuckLake supports integration with any storage system like local disk, local NAS, S3, Azure Blob Store, GCS, etc. The storage prefix for data files (e.g., s3://mybucket/mylake/) is specified when the metadata tables are created.

Finally, the SQL database that hosts the catalog server can be any halfway competent SQL database that supports ACID and primary key constraints. Most organizations will already have a lot of experience operating a system like that. This greatly simplifies deployment as no additional software stack is needed beyond the SQL database. Also, SQL databases have been heavily commoditized in recent years, there are innumerable hosted PostgreSQL services or even hosted DuckDB that can be used as the catalog store! Again, the lock-in here is very limited because transitioning does not require any table data movement, and the schema is simple and standardized.

There are no Avro or JSON files. There is no additional catalog server or additional API to integrate with. It’s all just SQL. We all know SQL.

Scalability
DuckLake actually increases separation of concerns within a data architecture into three parts. Storage, compute and metadata management. Storage remains on purpose-built file storage (e.g., blob storage), DuckLake can scale infinitely in storage.

An arbitrary number of compute nodes are querying and updating the catalog database and then independently reading and writing from storage. DuckLake can scale infinitely regarding compute.

Finally, the catalog database needs to be able to run only the metadata transactions requested by the compute nodes. Their volume is several orders of magnitude smaller than the actual data changes. But DuckLake is not bound to a single catalog database, making it possible to migrate e.g. from PostgreSQL to something else as demand grows. In the end, DuckLake uses simple tables and basic, portable SQL. But don’t worry, a PostgreSQL-backed DuckLake will already be able to scale to hundreds of terabytes and thousands of compute nodes.

Again, this is the exact design used by BigQuery and Snowflake that successfully manage immense datasets already. And hey, nothing keeps you from using Spanner as the DuckLake catalog database if required.

Speed
Just like DuckDB itself, DuckLake is very much about speed. One of the biggest pain points of Iceberg and Delta Lake is the involved sequence of file IO that is required to run the smallest query. Following the catalog and file metadata path requires many separate sequential HTTP requests. As a result, there is a lower bound to how fast reads or transactions can run. There is a lot of time spent in the critical path of transaction commits, leading to frequent conflicts and expensive conflict resolution. While caching can be used to alleviate some of these problems, this adds additional complexity and is only effective for “hot” data.

The unified metadata within a SQL database also allows for low-latency query planning. In order to read from a DuckLake table, a single query is sent to the catalog database, which performs the schema-based, partition-based and statistics-based pruning to essentially retrieve a list of files to be read from blob storage. There are no multiple round trips to storage to retrieve and reconstruct metadata state. There is also less that can go wrong, no S3 throttling, no failing requests, no retries, no not-yet consistent views on storage that lead to files being invisible, etc.

DuckLake is also able to improve the two biggest performance problems of data lakes: small changes and many concurrent changes.

For small changes, DuckLake will dramatically reduce the number of small files written to storage. There is no new snapshot file with a tiny change compared to the previous one, there is no new manifest file or manifest list. DuckLake even optionally allows transparent inlining of small changes to tables into actual tables directly in the metadata store! Turns out, a database system can be used to manage data, too. This allows for sub-millisecond writes and for improved overall query performance by reducing the number of files that have to be read. By writing many fewer files, DuckLake also greatly simplifies cleanup and compaction operations.

In DuckLake, table changes consist of two steps: staging the data files (if any) to storage, and then running a single SQL transaction in the catalog database. This greatly reduces the time spent in the critical path of transaction commits, there is only a single transaction to run. SQL databases are pretty good at de-conflicting transactions. This means that the compute nodes spend a much smaller amount of time in the critical path where conflicts can occur. This allows for much faster conflict resolution and for many more concurrent transactions. Essentially, DuckLake supports as many table changes as the catalog database can commit. Even the venerable Postgres can run thousands of transactions per second. One could run a thousand compute nodes running appends to a table at a one-second interval and it would work fine.

In addition, DuckLake snapshots are just a few rows added to the metadata store, allowing for many snapshots to exist at the same time. There is no need to proactively prune snapshots. Snapshots can also refer to parts of a Parquet file, allowing many more snapshots to exist than there are files on disk. Combined, this allows DuckLake to manage millions of snapshots!

Features
DuckLake has all of your favorite Lakehouse features:

Arbitrary SQL: DuckLake supports all the same vastness of SQL features that e.g. DuckDB supports.
Data Changes: DuckLake supports efficient appends, updates and deletes to data.
Multi-Schema, Multi-Table: DuckLake can manage an arbitrary number of schemas that each contain an arbitrary number of tables in the same metadata table structure.
Multi-Table Transactions: DuckLake supports fully ACID-compliant transactions over all of the managed schemas, tables and their content.
Complex Types: DuckLake supports all your favorite complex types like lists, arbitrarily nested.
Full Schema Evolution: Table schemas can be changed arbitrarily, e.g., columns can be added, removed, or have their data types changed.
Schema-Level Time Travel and Rollback: DuckLake supports full snapshot isolation and time travel, allowing to query tables as of a specific point in time.
Incremental Scans: DuckLake supports retrieval of only the changes that occurred between specified snapshots.
SQL Views: DuckLake supports the definition of lazily evaluated SQL-level views.
Hidden Partitioning and Pruning: DuckLake is aware of data partitioning and table- and file-level statistics, allowing for early pruning of scans for maximum efficiency.
Transactional DDL: Schema and table and view creation, evolution and removal are fully transactional.
Data Compaction Avoidance: DuckLake requires far fewer compaction operations than comparable formats. DuckLake supports efficient compaction of snapshots.
Inlining: When making small changes to the data, DuckLake can optionally use the catalog database to store those small changes directly to avoid writing many small files.
Encryption: DuckLake can optionally encrypt all data files written to data store, allowing for zero-trust data hosting. Keys are managed by the catalog database.
Compatibility: The data and (positional) deletion files that DuckLake writes to storage are fully compatible with Apache Iceberg allowing for metadata-only migrations.
Conclusion
We released DuckLake v0.1 with the ducklake DuckDB extension as its first implementation. We hope that you will find DuckLake useful in your data architecture – we are looking forward to your creative use cases!

Documentation
Manifesto
Specification
DuckLake DuckDB extension
DuckDB
DuckDB home
DuckDB installation
Development
Issues
Discussions
© 2025 DuckDB Foundation





