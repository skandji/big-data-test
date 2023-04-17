package kafka

import org.apache.kafka.clients.consumer.{CommitFailedException, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.logging.log4j.{LogManager, Logger}

import java.time.Duration
import java.util.{Collections, Properties}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object KafkaConsumer {

  val LOGGER: Logger = LogManager.getLogger("KAFKA_CONSUMER")

  def getKafkaComsumerParams (kafkaBootStrapServers : String, kafkaConsumerGroupID : String) : Properties = {

    val props : Properties = new Properties()

    props.put("bootstrap.servers", kafkaBootStrapServers)
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", kafkaConsumerGroupID)
    props.put("enable.auto.commit", "true")
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[StringDeserializer])

    props
  }

  def getKafkaConsumer(kafkaBootStrapServers : String, kafkaConsumerGroupID : String, topic_list : String) : KafkaConsumer[String, String] = {
    val consumer : KafkaConsumer[String, String] = new KafkaConsumer(getKafkaComsumerParams(kafkaBootStrapServers, kafkaConsumerGroupID))

    try{
      consumer.subscribe(Collections.singletonList(topic_list))

      while(true){
        val messages : ConsumerRecords[String, String] = consumer.poll(1000)
        if(!messages.isEmpty){
          LOGGER.info("Nombre de messages collectés dans la fénêtre:" +messages.count())
          for (record <- messages.asScala){
            println("Topic : " +record.topic()+
              ", key : "+record.key()+
              ", Value : "+record.value()+
              ",Offset :"+record.offset()+
              ", Partition : "+record.partition())

          }

          // Deuxième méthode de récupération des données dans le Log Kafka
          /*val recordIterator = messages.iterator()
          while(recordIterator.hasNext == true){
            val record = recordIterator.next()
            println("Topic : " +record.topic()+
              ", key : "+record.key()+
              ", Value : "+record.value()+
              ",Offset :"+record.offset()+
              ", Partition : "+record.partition())
          }*/
          try{
            consumer.commitAsync()
          } catch {
            case ex : CommitFailedException =>
              LOGGER.error(s"erreur dans le commit des offset, kafka n'a pas reçu le jeton de reconnaissance confirmant que nous avons reçu les données ${ex.printStackTrace()}")

          }

        } else {
          LOGGER.info("EMPTY MESSAGES")
        }

      }

    } catch {
      case excpt : Exception =>
        LOGGER.error(s"erreur dans le consumer ${excpt.printStackTrace()}")

    }finally {
      consumer.close()
    }

    consumer
  }
}
