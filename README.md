
<p align="center">
  <img src="retirement-age.png" alt="retirement-age.png" />


  <h3 align="center"></h3>

  <p align="center">
    Hadoop Data Lifecycle Automation
    <br>

  </p>
</p>

<br>


# Retirement Age
The Retirement Age data-lifecycle application is used to remove dataset records past 
an expiration date. Retirement age currently works with Parquet using the Hive Metastore.

## How it works
Retirement Age uses Spark to read Hive Metastore based tables. Based on a timestamp column that 
represents a creation date and an age (in days), retirement age will filter out all records that are
past their lifespan. 

1. Read in a dataset and filter out data that is older than its `expiration_column` + `expiration_days`
2. Write out the data to a new location
3. Change the table to point at the new location

Existing long running queries will not be affected because data is not changed in-place.  
Users will read from the new data on their next query (or when they invalidate metadata in Impala).

***note*** currently no data is deleted until the retirement process is run twice, the first to move 
filtered data to a new location, the second time to move it back to the original location, overwriting
the original data.

Datasets with records that don't have a expiration time can be removed if they can be linked to a
record that does have an expiration. For example, if you have a fact table with a foriegn key to a
dimension table, and the fact table record has a record removed, Retirement Age will make the join
to the dimension table and remove the dimension table record. See the `related_tables`
section in the yaml example below. `related tables` need a join key instead of an expiration column
and expiration date.

1. Read in a dataset that's older than its `expiration_column` + `expiration_days`
2. Join that dataset on a child/dimension table on the records that are left
3. Write out the child table to a new location and change the table location to point at the new/
filtered data
4. Write out the filtered parent table data and change its table location to point at the new/filtered
data

Child tables can be arbitrarily nested, so you can have a parent table that points to a child table,
and a child table that points to a grandchild table.

parent -> child -> grandchild

## Running the application
The application requires Apache Spark 2 to take advantages of new integrations with the Hive Metastore.

```bash
spark2-submit --deploy-mode client --master yarn --class io.phdata.retirementage.RetirementAge <path-to-jar> --conf <path-to-retirement-age.yml>
```

Flags: 

```bash
  -c, --conf  <arg>   Yaml formatted configuration file
      --counts        Whether to compute table counts pre/post filtering. This
                      will add to the run time and resource usage of the job.
  -d, --dry-run       Print out table counts and simulated actions for each
                      table but don't do anything real
  -u, --undo          Undo table location changes. Effectively undo deletes.
                      Deletes cannot be undone after the application has been
                      run twice.
      --help          Show help message
```


## Configuration file
Retirement age uses a yamlconfiguration file 'retirement-age.yml'


```yaml

databases: // list of databases
  - name: database1 // name of database
    tables: // list of tables
      - name: fact1 // REQUIRED name of table (required)
        storage_type: parquet // REQUIRED type of storage (currently only supports non-kudu Hive/Impala tables)
        expiration_column: col1 // REQUIRED Date column to compare for record removal. This can be a Date, Timestamp, Unix time seconds and Unix time milliseconds, and String 
        expiration_days: 100 // REQUIRED number of days from the date in `expiration_column` that the record will be removed
        hold: false // OPTIONAL when a hold is on a table no records will be removed
        date_format_string: 'yyyy-MM-dd' // OPTIONAL Custom date format string
      - name: dim1
        storage_type: parquet
        expiration_column: col1
        child_tables:
          - name: parquet2
            storage_type: parquet
            join_on:
              parent: col1
              self: col1

```

## Reporting
The application will create a report showing the original dataset sizes and new dataset sizes called
a 'retirement report'. The retirement reports are formatted in Markdown.
An example:

```bash
- qualifiedTableName: codb.users
  recordsRemoved: true
  originalDataset:
    location: hdfs://data/codb/users
    count: 123422
  newDataset:
    location: hdfs://data/codb/users_ra
    count: 114322
- qualifiedTableName: codb.web
  recordsRemoved: true
  originalDataset:
    location: hdfs://data/codb/web
    count: 45623345
  newDataset:
    location: hdfs://data/codb/web_ra
    count: 41345545   

```

## Running tests
```bash
$ sbt test
```

## Building
Create a fat jar
```bash
$ sbt assembly
```

## Roadmap
- Retire Kudu records
- Retire entire databases
- Cloudera Navigator integration

