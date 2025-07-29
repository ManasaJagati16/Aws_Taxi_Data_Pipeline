CREATE EXTERNAL TABLE nyc_taxi_zone_lookup (
  LocationID INT,
  Borough STRING,
  Zone STRING,
  service_zone STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = ','
)
LOCATION 's3://nyc-taxi-data-lake-manasa/lookup/'  -- Or your correct folder!
TBLPROPERTIES ('skip.header.line.count'='1');