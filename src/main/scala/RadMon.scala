import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

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

    val colName:String = "lastValue"
    val colType:String = "int"

    val result:Dataset[Row] = joined.select(colName).withColumn(colName,col(colName).cast(colType)).distinct().sort(colName)

    result.show(false)

    println(result.count())
    //cleanedMetaData.show()
  }

}
