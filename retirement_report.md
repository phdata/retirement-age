The application will create a report showing the original dataset sizes and new dataset sizes called a 'retirement report'.
The retirement report will also show information regarding the original dataset location and new dataset location. The
retirement reports are formatted in Markdown. An example:

```
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