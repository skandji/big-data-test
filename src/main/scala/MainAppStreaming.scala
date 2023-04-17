import kafka.{KafkaConsumer, KafkaProducer}
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.types.{StringType, StructType}
import streaming.{SparkKafkaConsumer, SparkKafkaProducer, StreamDataPipe}
import utilities.UtilsSpark

object MainAppStreaming {

  val LOGGER: Logger = LogManager.getLogger("MAIN_STREAMING")

  def main(args: Array[String]): Unit = {
    SparkKafkaProducer.getProducerKafka("localhost:9092", "Messages-2",
      "Streaming Kafka")

    val topics = Array("Messages")

    val sparkSession = UtilsSpark.sparkSession()

    /*val stream = SparkKafkaConsumer.getKafkaConsummer("localhost:9092", "group_01",
      "from-beginning", "localhost:2181", "", 3, topics)
    stream.map(record => (record.key(), record.value()))*/

    // KafkaProducer.getKafkaProducer("localhost:9092", "Messages", "Hello")
    // KafkaConsumer.getKafkaConsumer("localhost:9092", "group_04", "Messages")

    val schema = new StructType()
      .add("name", StringType)
    StreamDataPipe.readFromKafka(sparkSession,"localhost:9092", "iphone")

    val data = Seq (("iphone", "2007"),("iphone 3G","2008"),
      ("iphone 3GS","2009"),
      ("iphone 4","2010"),
      ("iphone 4S","2011"),
      ("iphone 5","2012"),
      ("iphone 8","2014"),
      ("iphone 10","2017"))

    /*val df = sparkSession.createDataFrame(data).toDF("key","value")
    StreamDataPipe.writeTokafka(df, "localhost:9092", "iphone")*/
  }
}
