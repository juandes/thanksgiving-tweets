import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

object GetTweets {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KeywordTweets <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)    
    
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf()
                     .setAppName("Get tweets")
                     .setMaster("spark://Juande.local:7077")
                     .set("spark.executor.memory", "1g")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None, filters)
    
    // Get the tweet, and if it is a retweet
    val tweets = stream.map(status => status.getText.replace("\n", ""))
    tweets.saveAsTextFiles("tweetsdata/tweetsStorage")
                 
    tweets.print()

    ssc.start()
    ssc.awaitTermination()
  }
}