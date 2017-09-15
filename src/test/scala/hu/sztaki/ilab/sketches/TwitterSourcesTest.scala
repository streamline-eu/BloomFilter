package hu.sztaki.ilab.sketches

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test

class TwitterSourcesTest extends StreamingMultipleProgramsTestBase with Serializable {
  @Test
  def testCsvFileSource(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val filename = getClass.getResource("/rg17_tweets_eng-first_10.csv").getPath
    val tweetStream = TwitterSources.csvTweetSourceWithTimestamps(env, filename)
    tweetStream.addSink(tweet => {
      val (id, text) = tweet
      println("" + id + ", " + text)
    })
    env.execute("TweetSourcesTest exampleCsv")
  }
}
