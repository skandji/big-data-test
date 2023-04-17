package streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import utilities.UtilsSpark.getSparkStreamingContext

import java.util.Properties

object SparkKafkaConsumer {

  val traceLog: Logger = LogManager.getLogger("Spark_Kafka_Consumer_BD")

  def  getSparkKafkaConsumerParams1(kafkaBootStrapServer : String, kafkaConsumerGroupId : String, kafkaConsumerReadOrder : String,
                                   kafkaZookeeper : String, kafkaKerberosName : String) : Properties ={
    val props = new Properties()

    props.put("bootstrap.servers", kafkaBootStrapServer)
    props.put("group.id",kafkaConsumerGroupId)
    props.put("zookeeper.hosts", kafkaZookeeper)
    props.put("auto.offset.reset", kafkaConsumerReadOrder)
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[StringDeserializer])
    props.put("sasl.kerberos.service.name", kafkaKerberosName)
    // props.put("security.protocol", SecurityProtocol.PLAINTEXT)

    props
  }

  def  getSparkKafkaConsumerParams2(kafkaBootStrapServer : String, kafkaConsumerGroupId : String, kafkaConsumerReadOrder : String,
                                   kafkaZookeeper : String, kafkaKerberosName : String) : Map[String, Object] = {
    val kafkaParam : Map[String, Object] =  Map(
      "bootstrap.servers" -> kafkaBootStrapServer,
      "group.id" -> kafkaConsumerGroupId,
      "zookeeper.hosts" -> kafkaZookeeper,
      "auto.offset.reset" -> kafkaConsumerReadOrder,
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "sasl.kerberos.service.name" -> kafkaKerberosName,
      // "security.protocol" -> SecurityProtocol.PLAINTEXT
    )

    kafkaParam
  }

  def getKafkaConsummer (kafkaBootStrapServer : String, kafkaConsumerGroupId : String,
                         kafkaConsumerReadOrder : String, kafkaZookeeper : String,
                         kafkaKerberosName : String, batchDuration : Int,
                         kafkaTopics : Array[String]) : InputDStream[ConsumerRecord[String, String]] = {
    var consumerKafka : InputDStream[ConsumerRecord[String, String]] = null
    var props: Map[String, Object] = null
    try{

      val ssc = getSparkStreamingContext(env= true, batchDuration)
      props = getSparkKafkaConsumerParams2(kafkaBootStrapServer, kafkaConsumerGroupId, kafkaConsumerReadOrder, kafkaZookeeper, kafkaKerberosName)
      consumerKafka = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](kafkaTopics, props)
      )

    }catch{
      case ex : Exception =>
        traceLog.error(s"erreur dans l'initialisaton du consumer kafka ${ex.getMessage}")
        traceLog.info(s"La liste des paramètres pour la connexion du consumer kafka sont : $props")
    }

    consumerKafka
  }
}
