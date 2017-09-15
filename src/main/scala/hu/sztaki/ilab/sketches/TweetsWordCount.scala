package hu.sztaki.ilab.sketches

import scala.util.Try
import org.apache.flink.api.scala._
import org.apache.flink.api.common.operators.Order

object TweetsWordCount {

  // Tweet to words definition: regex (\p{L}|\p{N})+
  private def tweetToWords(tweetStr: String): List[String] =
    """(\p{L}|\p{N})+""".r().findAllIn(tweetStr).map(_.toLowerCase).toList

  private def tweetWordCounts(input: String, output: String,
                              minOccs: Int, writeOccs: Boolean = false) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val counts = TwitterSources.csvTweetDataSet(env, input)
      // TODO: use stopwords instead of this filter:
      .flatMap(tweet => tweetToWords(tweet._2).filter(_.length > 3).map((_, 1)))
      .groupBy(0)
      .sum(1)
    val sortedCounts = counts.partitionByRange(1).sortPartition(1, Order.DESCENDING)
      .filter(_._2 >= minOccs)
    if (writeOccs) {
      sortedCounts.writeAsCsv(output, "\n", " ").setParallelism(1)
    } else {
      sortedCounts.map(_._1).writeAsText(output).setParallelism(1)
    }
    env.execute("TweetDataSet WordCount")
  }

  def main(args: Array[String]) {
    println("Given args: " + args.mkString(" "))
    assert(args.length >= 2, "Usage: PROG_NAME INPUT OUTPUT [MIN_OCCS] [WRITE_OCCS]")
    assert(new java.io.File(args(0)).exists(), "File " + args(0) + " does not exist!")
    assert(! new java.io.File(args(1)).exists(), "File " + args(1) + " already exists!")
    val minOccs = Try(args(2).toInt).getOrElse(0)
    val writeOccs = Try(args(3).toBoolean).getOrElse(false)
    println(" Used args: " + (args(0), args(1), minOccs, writeOccs).productIterator.mkString(" "))

    tweetWordCounts(args(0), args(1), minOccs, writeOccs)
  }
}
