from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def clean_data(df: DataFrame) -> DataFrame:
    cleaned_df = df.filter((col("amount") > 0) & (col("name").isNotNull())) \
                   .withColumn("amount_with_tax", col("amount") * 1.20)
    
    return cleaned_df