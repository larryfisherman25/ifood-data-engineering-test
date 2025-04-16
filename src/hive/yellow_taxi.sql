CREATE EXTERNAL TABLE IF NOT EXISTS yellow_taxi (
    VendorID INT,
    passenger_count DOUBLE,
    total_amount DOUBLE,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3a://prd-yellow-taxi-table-larry-test/processed/';

-- Após a inserção dos dados, execute para registrar as partições:
MSCK REPAIR TABLE yellow_taxi;
