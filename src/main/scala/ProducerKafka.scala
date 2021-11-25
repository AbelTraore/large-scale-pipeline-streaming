import com.twitter.hbc.core.endpoint.Location
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.ProducerConfig
//import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{LogManager, Logger}

import java.util._
import org.apache.kafka.common.security.auth.SecurityProtocol.SASL_PLAINTEXT


object ProducerKafka {

 private val trace_kafka = LogManager.getLogger("console")

  def getProducerKafka(message : String, topic : String, serversIP : String, ville : Location): Unit = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serversIP)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "import org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "import org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put("security.protocol", "SASL_PLAINTEXT")


    try
    {
      val twitterProducer = new KafkaProducer[String, String](props)
      val twitterRecord = new ProducerRecord[String, String](topic, ville.toString, message)
      twitterProducer.send(twitterRecord)

    }
    catch {
      case ex : Exception => trace_kafka.error(s"erreur dans l'envoi du message. Détails de l'erreur : ${ex.printStackTrace()}")

    }


  }







}
