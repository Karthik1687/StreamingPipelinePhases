package streamingtemplate

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object sampletemplate {

  // Setting up Spark Session
  val spark = SparkSession.builder()
    .appName("Stream Template")
    .master("local[2]")
    .getOrCreate()

  //  Collection Phase: Collection source can be any of the supported types. Here, it is from socket.
  //  But as mentioned in blog, it could be anything from kafka, queues, files. See the doc for more details.
  def collectData() = {
    // reading DataFrame
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 10000)
      .load()

    // Processing Phase: Any transformation or enrichment will be done in this phase. Rich set of APIs are supported in
    // in spark and can be used here. It will be applied on the incoming data once the stream starts.
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 10)


    // Consumption Phase: Consumption can be any sink or the target where the processed data will be outputted to.
    // console is one type of sink and we have large number of sinks - kafka, file etc., see doc for more details.
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start() // This is the place where the stream would be started

    // Stream continuously reads and listens to new data till external interruption or error.
    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    collectData()
  }
}
