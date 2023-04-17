package streaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.logging.log4j.{LogManager, Logger}

import java.util.Properties

object SparkKafkaProducer {

  val LOGGER: Logger = LogManager.getLogger("Spark_Kafka_Producer_BD")

  def getSparkKafkaProducerParams (kafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("bootstrap.servers", kafkaBootStrapServers)
    //props.put("security.protocol", "SASL_PLAINTEXT")

    props
  }

  def getProducerKafka (kafkaBootStrapServers : String, topicName : String, message : String) : KafkaProducer[String, String] = {

    LOGGER.info("Instanciation d'une instance du producer Kafka aux serveurs" + kafkaBootStrapServers)
    val producerKafka = new KafkaProducer[String, String](getSparkKafkaProducerParams(kafkaBootStrapServers))

    LOGGER.info(s"Message à publier dans le topic ${topicName}, ${message}")
    val recordPublish = new ProducerRecord[String,String](topicName, message)

    try {

      LOGGER.info("Publication du message")
      producerKafka.send(recordPublish)

    } catch{
      case ex : Exception =>
        LOGGER.error(s"erreur dans la publication du message dans kafka ${ex.printStackTrace()}")
        LOGGER.info(s"La liste des paramètres pour la connexion du producer kafka sont : ${getSparkKafkaProducerParams(kafkaBootStrapServers)}")
    } finally {

      producerKafka.close()
    }

    producerKafka

  }
}
