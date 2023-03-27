import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object RadMon {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("RadMon")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val radiationDF:Dataset[Row] = spark.read.text("E://radstat/*").withColumn("filename", input_file_name())

   // val radiationDF: Dataset[Row] = spark.read.parquet("E://radstat/*")
    radiationDF.show(false)

    val filtered:Dataset[Row] =  radiationDF.filter("length(value) != 0") // delete empty columns
                                            .filter(!col("value").contains( "1 year radiation")) // delete comment column
                                            .filter(!col("value").contains( "Datetime"))         // delete header column
                                            .withColumn("datetime",split(col("value"),",").getItem(0))
                                            .withColumn("cpm",split(col("value"),",").getItem(1))
                                            .withColumn("filename",reverse(split(col("filename"),"[\\/.]")).getItem(1)) // get filename one before last item in path string
                                            .drop("value")

    filtered.show(100,truncate = false)
    filtered.printSchema()


    //filtered.write.mode("overwrite").parquet("radstat.parquet")
    //filtered.write.mode("overwrite").csv("radstat.csv")

  }

}
