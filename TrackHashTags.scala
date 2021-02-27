import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._

object TrackHashTags{
  def main(args: Array[String]): Unit = {

    val sparkcontextobj = new SparkConf().setAppName("Twitter Streaming").setMaster("local[4]")
    val sc = new SparkContext(sparkcontextobj)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    System.setProperty("twitter4j.oauth.consumerKey", args(0))
    System.setProperty("twitter4j.oauth.consumerSecret", args(1))
    System.setProperty("twitter4j.oauth.accessToken", args(2))
    System.setProperty("twitter4j.oauth.accessTokenSecret", args(3))
    val ssc = new StreamingContext(sc, Seconds(2))
    val data_stream = TwitterUtils.createStream(ssc, None).filter(s=>s.getLang=="en")
    val getHashes = data_stream.flatMap(s => Predef.refArrayOps(s.getHashtagEntities))

    var hashes = getHashes.map(h =>"#"+ h.getText)
    val pairs_of_hashes: DStream[(String, Int)] = hashes.map(h =>(h, 1))

    val top5 = {
      pairs_of_hashes reduceByKeyAndWindow((k, w) => {
        k + w
      }, Seconds(120))
    }

    val top_elements: DStream[(String, Int)] = top5.transform(rdd => rdd.sortBy(x => x._2, false))
    top_elements.foreachRDD(foreachFunc = rdd => {
      val top_elemets_list: Array[(String, Int)] = rdd.take(5)
      Predef.refArrayOps(top_elemets_list).foreach { case (y) => println(s"${y._1}, count: ${y._2}") }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}