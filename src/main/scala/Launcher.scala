import com.twitter.hbc.core.endpoint.Location
import org.apache.log4j.{BasicConfigurator, LogManager, Logger}

import java.util.Properties
import scala.io.Source



object Launcher extends App {

  private var logger : Logger = LogManager.getLogger("Log_Console")
  //BasicConfigurator.configure();


  val url = getClass.getResource("streaming.properties")
  val source = Source.fromURL(url)
  val properties : Properties = new Properties()
  properties.load(source.bufferedReader())

  val consumer_key = properties.getProperty("consumerKey")
  val consumer_secret = properties.getProperty("consumerSecret")
  val access_token = properties.getProperty("token")
  val token_secret = properties.getProperty("tokenSecret")
  val serversKafka = properties.getProperty("bootStrapServers")
  val topicLecture = properties.getProperty("topic")

  val groupId = properties.getProperty("consumerGroupId")
  val readOrder = properties.getProperty("consumerReadOrder")
  val sparkBatchDuration = properties.getProperty("batchDuration")
  val mySQLHost_ = properties.getProperty("mySQLHost")
  val mySQLUser_ = properties.getProperty("mySQLUser")
  val mySQLPwd_ = properties.getProperty("mySQLPwd")
  val mySQLDatabase_ = properties.getProperty("mySQLDatabase")


  val montrealTweets = new Location(new Location.Coordinate(-73.972902,
    45.410076), new Location.Coordinate(-73.474295,45.70479))

  TwitterClient.getClient_Twitter(access_token, consumer_secret, consumer_key,
    token_secret, montrealTweets, topicLecture, serversKafka)




}
