import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, DateType
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GlueJobLogger")
logger.info("Glue job Started")

# Fetch parameters passed to the job
args = getResolvedOptions(sys.argv, [
    'bucket_path',
    'input_prefix',
    'avg_return_prefix',
    'most_traded_prefix',
    'volatile_prefix',
    'top_30_days_return_prefix'
])

bucket_path = args['bucket_path']
input_prefix = args['input_prefix']
avg_return_prefix = args['avg_return_prefix']
most_traded_prefix = args['most_traded_prefix']
volatile_prefix = args['volatile_prefix']
top_30_days_return_prefix = args['top_30_days_return_prefix']

# spark configs
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("spark.sql.shuffle.partitions", 1) # I Set to 1 due to small dataset, also no need for repartitioning
logger.info("Glue job initialized.")

# Define schema explicitly for better performance in big data scenarios
schema = StructType([
    StructField("Date", DateType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("ticker", StringType(), True)
])

df = spark.read.csv(f"{bucket_path}{input_prefix}", schema=schema, header=True) 
logger.info(f"Read file {input_prefix} successfully.")

# check Nulls in close and date cols - was used for data discovery
# df.printSchema
# df.filter(F.col('close').isNull()).show()
# df.filter(F.col('date').isNull()).show()
# df.show()

def write_output(df, output_path):
    """
    Writes a DataFrame to the specified S3 path as Parquet 
    P.S - Could have used CSV for better readability due to small dataset size but chose Parquet to demo a real-world scenario of big data.)
    """
    logger.info(f"Writing output file to {output_path}")
    df.write.parquet(output_path, mode="overwrite") # Could have partitioned by columns like ticker or date for better performance but didn't due to dataset size.

df = df.select("date", "volume", "ticker", "close")

# Create a window specification to order by date within each 'ticker' partition
window_spec = Window().partitionBy("ticker").orderBy("date")

# Add a new column 'previous_close' to represent the closing price from the closest previous date
df = df.withColumn("previous_close", F.lag("close").over(window_spec))

# Compute the percentage difference as (close - previous_close) / previous_close * 100
df = df.withColumn("Return", ((F.col("close") - F.col("previous_close")) / F.col("previous_close")) * 100).cache()

# Calculate the average return for each date
avg_return_df = df.groupBy("date").agg(F.avg("Return").alias("average_return"))

# avg_return_df.show()
write_output(avg_return_df, f"{bucket_path}{avg_return_prefix}")

# Calculate which stock was traded most frequently - Those are big numbers so pay attention to E (exponent)
most_traded_stock_df = df.withColumn("trade_frequency", F.col("close") * F.col("volume")) \
    .groupBy("ticker").agg(F.avg("trade_frequency").alias("frequency")) \
    .orderBy(F.desc("frequency")).limit(1)

# most_traded_stock_df.show()
write_output(most_traded_stock_df, f"{bucket_path}{most_traded_prefix}")

# Calculate the annualized standard deviation: use standard deviation and scale by the square root of trading days

trading_days_per_year = 252 # Many dates are missing in the csv, so I assumed the number to be 252 (NYSE and NASDAQ average).
volatility_df = df.groupBy("ticker").agg(
    (F.stddev("Return") * F.sqrt(F.lit(trading_days_per_year))).alias("annualized_std")
)

# Find the most volatile stock
most_volatile_stock_df = volatility_df.orderBy(F.desc("annualized_std")).limit(1)

# most_volatile_stock_df.show()
write_output(volatility_df, f"{bucket_path}{volatile_prefix}")

# Calculate the 30-day return
top_30_days_return_df = df.withColumn("30_days_ago_price", F.lag("close", 30).over(window_spec)) \
    .withColumn("30_days_return", ((F.col("close") - F.col("30_days_ago_price")) / F.col("30_days_ago_price")) * 100)

# Create a new window specification for ranking overall top three 30-day return dates
ranking_window_spec = Window().orderBy(F.desc("30_days_return"))

# Dense Rank the overall top three 30-day return dates (including ties)
top_30_days_return_df = top_30_days_return_df.withColumn("rank", F.dense_rank().over(ranking_window_spec)) \
.filter("rank <= 3").drop("rank","30_days_ago_price","close","30_days_return","volume","Return","previous_close")

# top_30_days_return_df.show()
write_output(top_30_days_return_df, f"{bucket_path}{top_30_days_return_prefix}")

logger.info("Glue job completed successfully.")
job.commit()