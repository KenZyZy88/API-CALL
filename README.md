## SPH Assessment
All files here are related to SPH Assessment. 


## Upcoming revisions

1. Change txt to use apache parquet.

Can be done by apache spark
```
       from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("TextToParquet").getOrCreate()

# Read the text file
text_df = spark.read.text("path/to/text/file.txt")

# Write the text file as a parquet file
text_df.write.parquet("path/to/parquet/file.parquet")
```
2. check on pagination 
3. backfield data
4. Rewrite process to support parquet. Use Apache spark
