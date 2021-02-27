import breeze.linalg.sum
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter._

object AveTweetLength {
  def main(args: Array[String]): Unit = {
    val sparkcontextobj = new SparkConf().setAppName( "Tweet_Average_length" ).setMaster( "local[2]" )
    val sc = new SparkContext( sparkcontextobj )
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel( Level.ERROR )
    System.setProperty("twitter4j.oauth.consumerKey", args(0))
    System.setProperty("twitter4j.oauth.consumerSecret", args(1))
    System.setProperty("twitter4j.oauth.accessToken", args(2))
    System.setProperty("twitter4j.oauth.accessTokenSecret", args(3))
    val ssc = new StreamingContext( sc, Seconds( 2 ) )
    val stream = TwitterUtils.createStream( ssc, None )
    val input_tweet = stream.map(s => s.getText())
     input_tweet.foreachRDD(foreachFunc = s => {
      val count_of_tweet = s.count()
       var sumArray = 0.0
       val out = s.map(a => a.length).collect()
       sumArray = sum(out)

       var avg = 0.0
       if (count_of_tweet !=0){
      avg = (sumArray/count_of_tweet)
         println("Total tweets:" + count_of_tweet + ", Average length: " + avg)
    }

    })

    ssc.start()
    ssc.awaitTermination()

  }
}