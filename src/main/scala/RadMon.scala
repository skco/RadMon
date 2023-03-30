import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object RadMon {



  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("RadMon")
                                          .master("local[*]")
                                          .getOrCreate()

    val loader  = new Loader(spark)
    val cleaner = new Cleaner(spark)

    val rawData     : Dataset[Row] =  loader.LoadRadMonData("E:/radstat/*")
    val cleanedData : Dataset[Row] =  cleaner.cleanRadMonData(rawData)

    val cleanedMetaData : Dataset[Row]  = cleaner.cleanMetaData("radmon.json")

    val joined:Dataset[Row] = cleanedData.join(cleanedMetaData,"StationName")

    val dropColumnList:Seq[String] = Seq("unknow1","unknow2","unknow3","unknow4","recordnumber","lastValue")

    val result:Dataset[Row] = joined.withColumn("conversionFactor",col("conversionFactor").cast(DoubleType))
                                    .withColumn("cpm",             col("cpm").cast(IntegerType))
                                    .withColumn("index",           col("index")           .cast(IntegerType))
                                    .withColumn("datetime",        to_timestamp(col("datetime")))
                                    .withColumn("lat",             col("lat").cast(DoubleType))
                                    .withColumn("lon",             col("lon").cast(DoubleType))
                                    .withColumn("uSv_hr",          col("cpm") * col("conversionFactor"))
                                    .drop(dropColumnList:_*)

    result.show(truncate = false)
    result.printSchema()

  }

}
