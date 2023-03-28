import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.input_file_name
import scala.io.Source

class Loader(spark:SparkSession) {
  /**
   *
   * @param filename - path to dir with radmon data
   * @return dataframe with loaded multiple files
   */
  def LoadRadMonData(dataDir:String):Dataset[Row]= {
     spark.read.text(dataDir).withColumn("filename", input_file_name())
  }
}
