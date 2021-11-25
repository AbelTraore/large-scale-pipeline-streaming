import com.twitter.hbc.httpclient.auth._
import com.twitter.hbc.core.{Client, Constants}
import com.twitter.hbc.core.endpoint.{Location, StatusesFilterEndpoint}

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import ProducerKafka._

import scala.collection.JavaConverters._
import org.apache.log4j.{LogManager, Logger}
//import KafkaProducerClient._


object TwitterClient {


  private val trace_hbc = LogManager.getLogger("console")



  def getClient_Twitter(token : String, consumerSecret : String, consumerKey : String, tokenSecret : String, town : Location, topicKafka : String, serveursKafka : String ): Unit = {


    val queue : BlockingQueue[String] = new LinkedBlockingQueue[String](1000000)
    val auth : Authentication = new OAuth1(consumerKey, consumerSecret, token, tokenSecret)

    val endp : StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endp.locations(List(town).asJava)

    val client_HBC = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .authentication(auth)
      .gzipEnabled(true)
      .endpoint(endp)
      .processor(new StringDelimitedProcessor(queue))

    val client_HBC_Complete = client_HBC.build()

    client_HBC_Complete.connect()

    try
    {
      while (!client_HBC_Complete.isDone) {
        val tweet = queue.poll(300, TimeUnit.SECONDS)
        getProducerKafka(tweet, topicKafka, serveursKafka, town)
      }
    } catch {
case ex : Exception => trace_hbc.error(s"erreur dans le client HBC. Détails : ${ex.printStackTrace()}")

    } finally {
client_HBC_Complete.stop()
    }

  }

}
