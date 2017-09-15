package hu.sztaki.ilab.sketches

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import purecsv.safe.CSVReader

import scala.io.Source

object TwitterSources {

  private implicit val typeIntStringInfo = TypeInformation.of(classOf[(Int, String)])
  private implicit val typeIntLongStringInfo = TypeInformation.of(classOf[(Int, Long, String)])
  private implicit val typeTweetObjInfo = TypeInformation.of(classOf[TweetObj])

  object VerySimpleIdGenerator {
    var id: Int = 0
    def gen: Int = { id += 1; id }
  }

  case class TweetObj(time: Long = 0, source_id: String = "", source_name: String = "",
                      language: String = "", text: String = "")

  private def csvTweetIterator(filename: String): Stream[TweetObj] = {
    Source.fromFile(filename).getLines().toStream.flatMap(
      line => CSVReader[TweetObj].readCSVFromString(line, '|').map(_.getOrElse(TweetObj())))
  }

  def csvTweetDataSet(env: ExecutionEnvironment, filename: String)
  : DataSet[(Int, String)] = {
    env.fromCollection(csvTweetIterator(filename))
      .map(tweetObj => (VerySimpleIdGenerator.gen, tweetObj.text))
  }

  def csvTweetSourceWithTimestamps(env: StreamExecutionEnvironment, filename: String)
  : DataStream[(Int, String)] = {
    env.fromCollection(csvTweetIterator(filename))
      .assignAscendingTimestamps(_.time)
      .map(tweetObj => (VerySimpleIdGenerator.gen, tweetObj.text))
  }

  def csvTweetSourceWithTime(env: StreamExecutionEnvironment, filename: String)
  : DataStream[(Int, Long, String)] = {
    env.fromCollection(csvTweetIterator(filename))
      .assignAscendingTimestamps(_.time)
      .map(tweetObj => (VerySimpleIdGenerator.gen, tweetObj.time, tweetObj.text))
  }
}