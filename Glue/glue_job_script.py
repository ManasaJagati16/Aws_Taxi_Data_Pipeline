import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import ApplyMapping
from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.functions import round
from pyspark.sql.functions import when
from pyspark.sql.functions import hour
from pyspark.sql.functions import to_date, month
from pyspark.sql.types import TimestampNTZType
from datetime import datetime
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame
import sys
import traceback
import boto3

# Generate timestamp folder

current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)




try:
    # Construct dynamic paths
    today_str = datetime.today().strftime('%Y-%m-%d')
    input_path = f"s3://nyc-taxi-data-lake-manasa/raw/nyc_taxi/{today_str}/"
    processed_output_path = f"s3://nyc-taxi-data-lake-manasa/processed/nyc_taxi/{today_str}/"

    # Read from dynamic raw S3 path
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [input_path]},
        format_options={"withHeader": True},
        transformation_ctx="dynamic_input"
    )
    
    # Explicitly raise error if S3 path is missing or no data is read
    if dyf.count() == 0:
        raise FileNotFoundError("No input files found in S3 — the path may be missing or empty")

    # Convert to DataFrame
    df = dyf.toDF()
    initial_count = df.count()

except FileNotFoundError as fnf:
    print(str(fnf))
    raise

except Exception as e:
    err_msg = str(e)
    if "Path does not exist" in err_msg or "No such file or directory" in err_msg:
        raise FileNotFoundError("S3 path missing or empty - No input files found in S3") from e
    else:
        print("Schema mismatch or unsupported format in Glue Data Catalog table 'nyc_taxi'.")
        traceback.print_exc()
        raise

try:
    #Filter out rows with negative or zero trip distance
    df = df.filter(col("trip_distance") > 0)
    

    # Calculate trip duration in minutes
    
    df = df.withColumn("trip_duration",
        (col("tpep_dropoff_datetime").cast("timestamp").cast("long") - col("tpep_pickup_datetime").cast("timestamp").cast("long")) / 60)
        
    # Round trip_duration to 2 decimal places
    df = df.withColumn("trip_duration", round(col("trip_duration"), 2))
    
    #Remove rows where any of these are null
    df = df.dropna(subset=["vendorid", "passenger_count", "trip_distance", "total_amount", "trip_duration"])
    
    #Get Final Count of dataframe after dropping the columns
    final_count = df.count()
    
    print("Initial row count:", initial_count)
    print("Final row count after cleaning:", final_count)
    print("Rows dropped:", initial_count - final_count)
    
    #Create two columns for Fair Efficiency dashboard
    df = df.withColumn("cost_per_minute", (col("total_amount") / col("trip_duration")))
    df = df.withColumn("cost_per_km", (col("total_amount") / col("trip_distance")))
    
    #Create a new column trip_category based on trip_duration (Short if <= 10min, Medium if > 10min and <= 30min, Long if > 30min)
    df = df.withColumn(
        "trip_category",
        when(col("trip_duration") <= 10, "Short")
        .when(col("trip_duration") <= 30, "Medium")
        .otherwise("Long")
    )
    
    #Extract the pickup hour from tpep_pickup_datetime
    df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    
    # Extract pickup_date (YYYY-MM-DD)
    df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
    
    # Filter only trips between Jan 1 and Mar 31, 2024
    df = df.filter(
        (col("pickup_date") >= to_date(lit("2024-01-01"))) &
        (col("pickup_date") <= to_date(lit("2024-03-31")))
    )
    
    # Extract pickup_month (1 to 12)
    df = df.withColumn("pickup_month", month(col("tpep_pickup_datetime")))
    
    safe_cols = [field.name for field in df.schema.fields if not isinstance(field.dataType, TimestampNTZType)]
    df_filtered = df.select(*safe_cols)
    
    # Now pick only what you want from that
    df_selected = df_filtered.select(
        "vendorid",
        "passenger_count",
        "trip_distance",
        "payment_type",
        "total_amount",
        "trip_duration",
        "trip_category",
        "pickup_hour",
        "pickup_date",
        "pickup_month",
        "cost_per_minute",
        "cost_per_km"
    )
    
    # Convert back to DynamicFrame
    dyf_filtered = DynamicFrame.fromDF(df_selected, glueContext, "dyf_filtered")
    #df_filtered = dyf_filtered.toDF().limit(100)
    #dyf_filtered = DynamicFrame.fromDF(df_filtered, glueContext, "dyf_filtered")
    
    #raise Exception("Simulated failure for CloudWatch alarm testing")
    
except Exception as e:
    print(f"Exception: {str(e)}")  # <-- This line gets written to CloudWatch Logs
    #raise  # Re-raise to mark Glue job as failed
    
# Write processed data to dynamic timestamp folder in S3 bucket
glueContext.write_dynamic_frame.from_options(
    frame=dyf_filtered,
    connection_type="s3",
    format="parquet",
    connection_options={"path": processed_output_path}
    #"partitionKeys": ["pickup_date"]
    
)

job.commit()



