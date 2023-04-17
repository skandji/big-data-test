package utilities

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.FileNotFoundException

object UtilsSpark {

  var ss: SparkSession = null
  var traceLog: Logger = LogManager.getLogger("Logger_Console")

  def sparkSession(env: Boolean = true): SparkSession = {
    try {
      if (env) {
        System.setProperty("hadoop.home.dir", "C:/Hadoop") // sur windows = "C:/Hadoop/"
        ss = SparkSession.builder()
          .master("local[*]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          .enableHiveSupport()
          .getOrCreate()
      } else {
        ss = SparkSession.builder()
          .appName("Mon application Spark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          .enableHiveSupport()
          .getOrCreate()
      }
      traceLog.info("DEMARRAGE DE LA SESSION SPARK")
    } catch {
      case ex: FileNotFoundException => traceLog.error(ex.printStackTrace())
      case ex: Exception => traceLog.error(ex.printStackTrace())
    }

    ss

  }

  def getSparkStreamingContext(env: Boolean = true, batchDuration: Int): StreamingContext = {
    traceLog.info("initialisation du contexte Spark Streaming")
    val sparkConf = new SparkConf()
    if (env) {
      sparkConf.setMaster("local[*]")
        .setAppName("Mon application streaming")
    } else {
      sparkConf.setAppName("Mon application streaming")
    }
    traceLog.info(s"la durée du micro-bacth Spark est définie à : $batchDuration secondes")
    val streamingContext : StreamingContext = new StreamingContext(sparkConf, Seconds(batchDuration))

    streamingContext
  }
}
