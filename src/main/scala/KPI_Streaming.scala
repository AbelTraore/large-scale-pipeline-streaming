import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import Launcher._




object KPI_Streaming {

  val kpiSchema = StructType ( Array(        // theses are the fields of a twitter objet
    StructField("event_date", DateType, true),
    StructField("id", StringType, false),
    StructField("text", StringType, true),
    StructField("lang", StringType, true),
    StructField("userid", StringType, false),
    StructField("name", StringType, false),
    StructField("screenName", StringType, true),
    StructField("location", StringType, true),
    StructField("followersCount", IntegerType, false),
    StructField("retweetCount", IntegerType, false),
    StructField("favoriteCount", IntegerType, false),
    StructField("Zipcode", StringType, true),
    StructField("ZipCodeType", StringType, true),
    StructField("City", StringType, true),
    StructField("State", StringType, true))
  )

  // parameters of the Spark consumer client, needed to connect to Kafka
  private val bootStrapServers : String = Launcher.serversKafka
  private val consumerGroupId : String = Launcher.groupId
  private val consumerReadOrder : String = Launcher.readOrder
  // private val kerberosName : String = ""
  private val batchDuration  = 300
  private val topics : Array[String] = Array(Launcher.topicLecture)
  private val mySQLHost = Launcher.mySQLHost_
  private val mySQLUser = Launcher.mySQLUser_
  private val mySQLPwd = Launcher.mySQLPwd_
  private val mySQLDatabase = Launcher.mySQLDatabase_

  private var logger : Logger = LogManager.getLogger("Log_Console")
  var ss : SparkSession = null
  var spConf : SparkConf = null


  def main(args: Array[String]): Unit = {

val kafkaParams = Map(
  "bootstrap.servers" -> bootStrapServers,
  "group.id"  -> consumerGroupId,
  "auto.offset.reset" -> consumerReadOrder,
  "enable.auto.commit" -> (false: java.lang.Boolean),
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "security.protocol" -> SecurityProtocol.PLAINTEXT
  // "sasl.kerberos.service.name" -> KerberosName
)

    val ssc = getSparkStreamingContext(batchDuration, true)

    try {

      val kk_consumer = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

      // lecture des événements reçus du cluster Kafka
      kk_consumer.foreachRDD{

        rdd_kafka => {
          val record_kafka = rdd_kafka.map(c => c.value())
          val offsets = rdd_kafka.asInstanceOf[HasOffsetRanges].offsetRanges


          val s_session = SparkSession.builder()
            .config(rdd_kafka.sparkContext.getConf)
            .enableHiveSupport()
            .getOrCreate()

          import s_session.implicits._
          val df_messages = record_kafka.toDF("kafka_objet")

          if(df_messages.count() > 0) {

            //parsing des données
            val df_parsed = messageParsing(df_messages)

            //calcul des indicateurs
            val df_indicateurs = computeIndicators(df_parsed, s_session).cache()

            // écriture des indicateurs dans MySQL
            df_indicateurs.write
              .format("jdbc")
              .mode(SaveMode.Append)
              .option("url", s"jdbc:mysql://${mySQLHost}?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
              .option("dbtable", mySQLDatabase)
              .option("user",mySQLUser )
              .option("password", mySQLPwd)
              .save()


          }

          kk_consumer.asInstanceOf[CanCommitOffsets].commitAsync(offsets)

        }

      }

    } catch {

case ex : Exception => logger.error(s"l'application Spark a rencontré une erreur fatale : ${ex.printStackTrace()}")

    } finally {

      ssc.start()
      ssc.awaitTermination()

    }



  }


  def getSparkStreamingContext(duree_batch : Int, env : Boolean) : StreamingContext = {
    logger.info("initialisation du contexte Spark Streaming")
    if (env) {      // si Spark est déployé en local
      spConf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("Mon app Streaming Dev")
    } else {
      spConf = new SparkConf().setAppName("Mon app Streaming Prod")

    }

    val ssc : StreamingContext = new StreamingContext(spConf, Seconds(duree_batch))

    return ssc

  }

  def messageParsing(df_kafkaEvents : DataFrame) : DataFrame = {
    logger.info("parsing des objets json reçus de Kafka encours...")

    val df_parsed = df_kafkaEvents.withColumn("kafka_objet", from_json(col("kafka_objet"), kpiSchema))

    df_parsed

  }

  def computeIndicators(event_parsed : DataFrame, ss : SparkSession) : DataFrame = {

    logger.info("calcul des KPI encours...")
    event_parsed.createTempView("messages")

    val df_kpi = ss.sql("""
                SELECT t.City,
                       count(t.id) OVER (PARTITION BY t.City ORDER BY City) as tweetCount,
                       sum(t.bin_retweet) OVER (PARTITION BY t.City ORDER BY City) as retweetCount
                FROM  (
                    SELECT date_event, id, City, CASE WHEN retweetCount > 0 THEN 1 ELSE 0 END AS bin_retweet  FROM events_tweets
                     ) t
        """
    ).withColumn("date_event", current_timestamp())
      .select(
        col("date_event").alias("date de l'événement"),
        col("city").alias("events city"),
        col("tweetCount").alias("Nbr de tweets par cité"),
        col("retweetCount").alias("Nbr de RT(retweets) par cité")
      )

    return df_kpi


  }



}
