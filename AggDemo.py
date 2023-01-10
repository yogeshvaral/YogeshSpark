from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("AggDemo") \
        .master("local[3]") \
        .getOrCreate()

    invoice_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    # invoice_df.show()
    invoice_df.select(f.count("*").alias("Count *"),
                        f.sum("Quantity").alias("quantity"),
                        f.avg("UnitPrice").alias("average Unit Price")
                      ).show()

    invoice_df.groupby("Country","InvoiceNo")\
                .agg(f.sum("Quantity").alias("total quantity"),
                     f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("Invoice_value"),
                     f.expr("round(sum(Quantity * UnitPrice),2) as invoice_value_expr")).show()

    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.round(f.sum(f.col("Quantity") * f.col("UnitPrice")),2).alias("InvoiceValue")
    invoice_df_weekly = invoice_df.withColumn("InvoiceDate", f.to_date("InvoiceDate", "dd-MM-yyyy H.mm"))\
        .where("year(InvoiceDate) == '2010'")\
        .withColumn("WeekNumber",f.weekofyear("InvoiceDate"))\
        .groupby("Country", "WeekNumber").agg(
        f.countDistinct("InvoiceNo").alias("invoiceCount"), TotalQuantity, InvoiceValue
    )
    # invoice_df_weekly.coalesce(1).write.format("parquet").mode("overwrite").save("output")
    sorted_df = invoice_df_weekly.sort("Country", "WeekNumber")
    sorted_df.show()

    running_total_window =  Window.partitionBy("Country").orderBy("WeekNumber").rowsBetween(Window.unboundedPreceding,Window.currentRow)
    sorted_df.withColumn("RunningTotal", f.sum("InvoiceValue").over(running_total_window)).show()