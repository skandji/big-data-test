package kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.logging.log4j.{LogManager, Logger}

import java.util.Properties

object KafkaProducer {

  val LOGGER: Logger = LogManager.getLogger("KAFKA_PRODUCER")

  def getKafkaProducerParams (kafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()

    props.put("key.serializer", classOf[StringSerializer])
    props.put("value.serializer",classOf[StringSerializer])
    props.put("acks", "all")
    props.put("bootstrap.servers", kafkaBootStrapServers)
    // props.put("security.protocol", "SASL_PLAINTEXT")

    props
  }

  def getKafkaProducer( kafkaBootStrapServers : String, topicName : String, message : String) : KafkaProducer[String, String] = {
    LOGGER.info(s"instanciation d'une instance du producer Kafka aux serveurs ${kafkaBootStrapServers}")
    lazy val producer = new KafkaProducer[String, String](getKafkaProducerParams(kafkaBootStrapServers))

    LOGGER.info(s"message à publier dans le topic ${topicName}, ${message}")
    val recordProducer = new ProducerRecord[String,String](topicName, message)

    try{
      LOGGER.info("publication du message encours ...")
      producer.send(recordProducer)
      LOGGER.info("message publié avec succés !:)")
    } catch {
      case ex : Exception =>
        LOGGER.error(s"erreur dans la publication du message ${message} dans le Log du topic ${topicName} dans kafka : ${ex.printStackTrace()}")
        LOGGER.info(s"la liste des paramètres pour la connexion du producer Kafka sont : ${getKafkaProducerParams(kafkaBootStrapServers)}")
    } finally {
      producer.close()
    }
    producer
  }
}
