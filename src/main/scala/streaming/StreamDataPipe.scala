package streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import utilities.UtilsSpark
import org.apache.spark.sql.functions.{col, from_json, get_json_object}

object StreamDataPipe {

  val LOGGER: Logger = LogManager.getLogger("STREAMING_DATA_PIPE")

  /**
   *
   * @param kafkaBootstrapServer
   * @param topic
   */
  def readFromKafka(sparkSession: SparkSession, kafkaBootstrapServer: String, topic: String, schema: StructType) = {
    try {
      val df = getDataFrameFromKafka(sparkSession, kafkaBootstrapServer, topic)
      LOGGER.info("DATAFRAME SCHEMA: ")
      df.printSchema()
      val stringDf = df.selectExpr("CAST(value AS STRING)")
      // val objectDf = stringDf.select(from_json(col("value").cast("string"), schema).as("data")).select("data.*")
      stringDf.writeStream
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination(10000)
    } catch {
      case ex : Exception => LOGGER.error(ex.getMessage)
    }
  }

  /**
   *
   * @param dataFrame
   * @param kafkaBootstrap
   * @param topic
   */
  def writeTokafka(dataFrame: DataFrame, kafkaBootstrap: String, topic: String) = {
    try {
      dataFrame.write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrap)
        .option("topic", topic)
        .save()
      LOGGER.info("WRITE TO SUCCESSFULLY")
    } catch {
      case ex : Exception => LOGGER.error(s"Exception: $ex.getMessage")
    }
  }

  /**
   *
   * @param sparkSession
   * @param kafkaBootstrapServer
   * @param topic
   * @return
   */
  def readFromKafka(sparkSession: SparkSession, kafkaBootstrapServer: String, topic: String) = {
    try {
      val df = getDataFrameFromKafka(sparkSession, kafkaBootstrapServer, topic)
      LOGGER.info("DATAFRAME SCHEMA: ")
      df.printSchema()
      val customDf = df.selectExpr("CAST(value AS STRING)", "CAST(key AS STRING)")
        .withColumnRenamed("value", "year")
        .withColumnRenamed("key", "name")
      customDf.writeStream
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination(10000)
    } catch {
      case ex : Exception => LOGGER.error(ex.getMessage)
    }
  }

  /**
   *
   * @param sparkSession
   * @param bootstrapServer
   * @param topic
   * @return
   */
  def getDataFrameFromKafka(sparkSession: SparkSession, bootstrapServer: String, topic: String):DataFrame = {
    try {
      val df = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServer)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
       return df

    } catch {
      case exception: Exception => LOGGER.error(exception.getMessage)
    }
    null
  }
}
