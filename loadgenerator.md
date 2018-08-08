## Retirement Age LoadGenerator
LoadGenerator is a tool used to create test data to be used as a load test for RetirementAge.
LoadGenerator can generate test tables stored in Parquet or Kudu.

### How it works
This tool uses spark to create three dataframes, a fact dataframe, a dimension dataframe, and a subdimension dataframe. Those
dataframes are then stored as the storage type you specify, currently either Parquet or Kudu. Parquet files are stored as
tables using Hive's Metadata, and Kudu tables are stored using Kudu's Spark API. This application creates three tables:
a fact-table, dimension-table, and subdimension-table. The fact-table is dated, while the dimension-table has a key to
join onto the fact-table. The subdimension-table has a key to join onto the dimension-table.

### Running the application
LoadGenerator requires you to input the number of records to create for all three tables, the database name, and the storage
type. The names for the fact, dimension, and subdimension tables are optional. If no name is specified for the three tables
then it will use the default names which are: factloadtest, dimloadtest, and subloadtest. The two storage type inputs that
are accepted are 'parquet' or 'kudu'.

```
spark2-submit --deploy-mode client --master yarn --class io.phdata.retirementage.loadgen.LoadGenerator <path-to-jar> /
--fact-count <#> --dimension-count <#> --subdimension-count <#> --database-name <name> --fact-name <name> --dim-name
<name> --subdim-name <name> --storage-type <parquet/kudu>
```