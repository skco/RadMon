import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions.countDistinct

object RadStat {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("RadStat")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val radiationDF:Dataset[Row] = spark.read.text("E://radstat/*").withColumn("filename", input_file_name())

   // val radiationDF: Dataset[Row] = spark.read.parquet("E://radstat/*")
    radiationDF.show(false)

    val filtered:Dataset[Row] =  radiationDF.filter("length(value) != 0")
                                            .filter(!col("value").contains( "1 year radiation"))
                                            .filter(!col("value").contains( "Datetime"))
                                            .withColumn("datetime",split(col("value"),",").getItem(0))
                                            .withColumn("cpm",split(col("value"),",").getItem(1))
                                            .withColumn("filename",reverse(split(col("filename"),"[\\/.]")).getItem(1))
                                            .drop("value")



    filtered.show(10000,truncate = false)

    //filtered.write.mode("overwrite").parquet("radstat.parquet")
    //filtered.write.mode("overwrite").csv("radstat.csv")

  }

}
