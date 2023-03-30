import org.apache.spark.sql.functions.{col, reverse, split}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec
import scala.io.{BufferedSource, Source}

class Cleaner(spark:SparkSession) {
  /**
   *
   * @param rawData - raw DataFrame - loader output
   * @return cleaned DataFrame
   */
  def cleanRadMonData(rawData:Dataset[Row]):Dataset[Row] = {
    val dropCols:Seq[String]  = Seq("unknow1","unknow2","unknow3","unknow4","recordNumber")

    rawData.filter("length(value) != 0") // delete empty columns
           .filter(!col("value").contains("1 year radiation")) // delete comment column
           .filter(!col("value").contains("Datetime")) // delete header column
           .withColumn("datetime", split(col("value"), ",").getItem(0))
           .withColumn("cpm", split(col("value"), ",").getItem(1))
           .withColumn("filename", reverse(split(col("filename"), "[\\/.]")).getItem(1)) // get filename one before last item in path string
           .drop("value")
           .withColumnRenamed("filename","StationName")
           .drop(dropCols:_*)

  }

  /**
   *
   * @param fileName = "broken" JSON filename
   * @return dataframe with metadata as rows
   */
  private def lengthMetaData(fileName:String):Int=  {
    import spark.implicits._
    val file = Source.fromFile(fileName)
    val fileLines = file.getLines()
    val firstLine:String =fileLines.next()
    file.close()
    firstLine.split("[,]+(?=(?:[^\\\"]*[\\\"][^\\\"]*[\\\"])*[^\\\"]*$)").length
  }

  /**
   *
   * @param fileName - filename with radmon metadata "broken json"
   * @return - Dataframe with metadata
   */
  def cleanMetaData(fileName:String): Dataset[Row] = {
    import spark.implicits._
    val columnsList: Seq[String] = Seq("index", "Location", "DeviceType", "unknow1", "unknow2", "unknow3", "URL", "lastValue",
                                       "unknow4", "recordNumber", "lat", "lon", "StationName", "conversionFactor")

        @tailrec
        def addRow(buf: Iterator[String], DF: Dataset[Row]): Dataset[Row] = {
              val line: String = buf.next() // get next line from itterator
              val acc: Dataset[Row] = DF.join(line.replace("[", "") // parse line
                .replace("]", "")
                .replace("\"", "")
                .split("[,]+(?=(?:[^\\\"]*[\\\"][^\\\"]*[\\\"])*[^\\\"]*$)") // split on "," only outside ""
                .toSeq.toDF()
                .withColumn("index", monotonically_increasing_id()), "index") // add index

              if (buf.hasNext) addRow(buf, acc) else acc            // recursive read next row or return final DF
            }

            val recordsCount:Int = lengthMetaData(fileName)         // get metatsada records
            val file: BufferedSource = Source.fromFile(fileName)
            val fileLines = file.getLines()

            val indexDF: Dataset[Row] = (0 until recordsCount).toDF().withColumnRenamed("value", "index")
            val result : Dataset[Row] = addRow(fileLines, indexDF).toDF(columnsList:_*)
            file.close()
            result
          }
}
