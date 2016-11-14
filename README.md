# digdag-bq-wait

digdag gcp plugin wait for table in Google BigQuery 

## How to use

### Create file
[test.dig]
```
timezone: Asia/Tokyo
_export:
  plugin:
    repositories:
      - https://gyam.bintray.com/maven/
    dependencies:
      - io.digdag.plugin.gcp:digdag-bq-wait:0.1.0

+step0:
  bq_wait>:
  dataset: test_dataset
  table: test_table
```

### Run

```
$ digdag run test.dig
```
