![Travis Build Tag](https://travis-ci.org/phdata/retirement-age.svg?branch=master)

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
The Retirement Age data-lifecycle application is an open source solution for removing dataset records past an expiration date.
It allows you to easily filter datasets stored in Parquet and Avro, using the Hive Metastore, or datasets stored in Kudu.
Retirement Age helps to easily meet federal regulations or liability reasons for data retention while filtering out
unneeded records or simply clean up old unused data.

[How Retirement Age works](#how-it-works).

## Core concepts
- Easily delete expired records from a dataset
- Existing long running queries on Avro or Parquet stored tables will not be affected, until Retirement Age is run twice.
- Datasets with records that don’t have an expiration time can be removed if they can be linked to a record that does have an expiration.

## Quickstart
1. [Build Retirement Age](#building-retirement-age)
2. [Setting up the config](#configuration)
3. [Running Retirement Age](#running-retirement-age)

- Note: When working with tables stored in Avro or Parquet no data is deleted until the retirement process is run twice.
The first run moves filtered data to a new location, and the second moves the data back to the original location, overwriting
the original data.

## Building Retirement Age
Retirement Age is built using [sbt](https://www.scala-sbt.org/). To build Retirement Age’s JAR and it’s dependencies run:
```
sbt assembly
```

## Configuration
Retirement Age uses a yaml configuration file ‘retirement-age.yml’. This configuration file holds the information to run
this application. In this configuration file you will store information regarding the tables you want to filter, the databases
the tables live in, and other crucial information. An example can be seen here:

```
kudu_masters: // REQUIRED if a Kudu table exists
  - kuduMaster1
  - kuduMaster2
  - kuduMaster3
databases: // list of databases
  - name: database1 // name of database (if a Kudu table does not belong to a database input '')
    tables: // list of tables
      - name: fact1 // REQUIRED name of table
        storage_type: parquet // REQUIRED type of storage (currently supports Hive/Impala tables and Kudu tables)
        expiration_column: col1 // REQUIRED Date column to compare for record removal. This can be a Date, Timestamp, Unix time seconds and Unix time milliseconds, and String
        expiration_days: 100 // REQUIRED number of days from the date in `expiration_column` that the record will be removed
        hold: false // OPTIONAL when a hold is on a table no records will be removed
        date_format_string: 'yyyy-MM-dd' // OPTIONAL Custom date format string
      - name: fact2
        storage_type: kudu
        expiration_column: col1
		  expiration_days: 100
        child_tables:
          - name: parquet2
            storage_type: kudu
            join_on:
              parent: col1
              self: col2
```

One of these crucial information configurations is **expiration_days**, which is used to calculate what records to filter out.
Retirement Age will filter out data that is older than its **expiration_column** + **expiration_days**, both of which you setup
with ‘retirement-age.yml’

## Running Retirement Age
```
spark2-submit --deploy-mode client --master yarn --class io.phdata.retirementage.RetirementAge <path-to-jar> --conf <path-to-retirement-age.yml>
```
Flags:
 ```
   -c, —conf  <arg>   Yaml formatted configuration file
       —counts        Whether to compute table counts pre/post filtering. This
                       will add to the run time and resource usage of the job.
   -d, —dry-run       Print out table counts and simulated actions for each
                       table but don’t do anything real
   -u, —undo          Undo table location changes. Effectively undo deletes.
                       Deletes cannot be undone after the application has been
                       run twice.
       —help          Show help message
 ```

## Running tests
To run unit tests:
```
$ sbt test
```
To run Kudu integration tests:
```
$ make integration-test
```

## Reporting
This application will create a report showing the original dataset sizes and new dataset sizes called a ‘retirement report’.
The retirement report will also hold information on the original dataset location and new dataset location. An example can
be found [here.](retirement_report.md)

## How it works
Retirement Age uses Spark to read Hive Metastore based tables, and uses Kudu’s Spark API to read Kudu tables. Based on a
timestamp column that represents a creation date and an age (in days), retirement age will filter out all records that are
past their lifespan. Retirement Age works slightly different between Hive Metastore based tables and Kudu tables.

Datasets with records that don’t have an expiration time can be removed if they can be linked to a record that does have an
expiration. For example, if you have a fact table with a foreign key to a dimension table, and the fact table record has a
record removed, Retirement Age will make the join to the dimension table and remove the dimension table record. See the child
table example in [Configuration](#configuration). Related tables need a join key instead of an expiration column and expiration
date. This join key will be used to join the Parent and Child table on.

parent -> child -> grandchild

### Hive Metastore Based Tables
1. Read in a dataset and filter out data that is older than its expiration_column + expiration_days
2. Join that dataset on a child/dimension table on the records that are left
3. Write out the data to a new location
4. Change the table to point at the new location

Existing long running queries will not be affected because data is not changed in-place. Users will read from the new data
on their next query (or when they invalidate metadata in Impala).

**Note:** For Hive Metastore based tables no data is deleted until the retirement process is run twice. The first run moves filtered data to a new location, and the second run moves it back to the original location, overwriting the original data.

Child Table Deletion:
1. Read in a dataset that's older than its expiration_column + expiration_days
2. Join that dataset on a child/dimension table on the records that are left
3. Write out the child table to a new location and change the table location to point at the new/ filtered data
4. Write out the filtered parent table data and change its table location to point at the new/filtered data

### Kudu Based Tables
1. Read in a dataset and filter out data that is older than its expiration_column + expiration_days
2. Join that table’s expired records on a child/dimension table
3. Delete the expired records from the Kudu Table

Existing long running queries could be affected because the data is being deleted on the first run of Retirement Age.

Child Table Deletion:
1. Read in a dataset that’s older than its expiration_column + expiration_days
2. Join that table’s expired records on a child/dimension table
3. Delete those expired records from the child/dimension table

## Known Issues
- You cannot match a Kudu parent table to a Kudu child table on columns with the same name

## Additional Features
- This application also comes with a LoadGenerator for both Parquet and Kudu stored tables.
For more information on how to use LoadGenerator click [here](loadgenerator.md).

## Roadmap
- Cloudera Navigator Integration