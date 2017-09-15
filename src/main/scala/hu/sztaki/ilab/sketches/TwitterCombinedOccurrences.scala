package hu.sztaki.ilab.sketches

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

import scala.util.Try

object TwitterCombinedOccurrences {

  private def dateStrToTimeStamp(dateStr: String): Long =
    java.time.LocalDateTime.parse(dateStr).toEpochSecond(java.time.ZoneOffset.ofHours(0))

  private def timeToHuman(t: Long): String =
    java.time.LocalDateTime.ofEpochSecond(t, 0, java.time.ZoneOffset.ofHours(0)).toString

  private def twitterCombOccs(wordsFilename: String, tweetsFilename: String, readsHost: String,
                              readsPort: Int, readsOutput: String,
                              hashMax: Int, hs: Array[Int => Int], numParts: Int): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val words = scala.io.Source.fromFile(wordsFilename).getLines().toList
    val bloomCombOcc = new BloomCombinedOccurrences(words, hashMax, hs, numParts)

    val writeStream = TwitterSources.csvTweetSourceWithTimestamps(env, tweetsFilename)
      .flatMap(new bloomCombOcc.BloomWriter)
    val readStream = env.socketTextStream(readsHost, readsPort)
      .flatMap(new bloomCombOcc.BloomReader)
    bloomCombOcc.applyCombOccs(writeStream, readStream)
      .map(r => {val (word, combOccs) = r; (word, combOccs.toSeq.sortWith(_._2 > _._2).take(100))})
      .writeAsText(readsOutput).setParallelism(1)
    env.execute()
  }

  private def windowedCombOccs(wordsFilename: String, tweetsFilename: String, readsHost: String,
                               readsPort: Int, readsOutput: String,
                               hashMax: Int, hs: Array[Int => Int], numParts: Int,
                               windowRanges: WindowRanges): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val words = scala.io.Source.fromFile(wordsFilename).getLines().toList
    val bloomCombOcc =
      new WindowedBloomCombinedOccurrences(words, hashMax, hs, numParts, windowRanges)

    val writeStream = TwitterSources.csvTweetSourceWithTime(env, tweetsFilename)
      .flatMap(new bloomCombOcc.BloomWriter)
    val readStream = env.socketTextStream(readsHost, readsPort)
      .map(str => { val inWords = Try(str.split(' '))
        ( inWords.map(_(0)).getOrElse(""),
          inWords.map(_(1)).map(dateStrToTimeStamp).toOption ) })
      .flatMap(new bloomCombOcc.BloomReader)
    bloomCombOcc.applyCombOccs(writeStream, readStream)
      .map(r => { val (wordOpt, windowOpt, combOccs) = r
        val windowHumanROpt = windowOpt.map(windowRanges.getInterval(_).map(timeToHuman).toString)
        (wordOpt, windowHumanROpt, combOccs.toSeq.sortWith(_._2 > _._2).take(100))})
      .writeAsText(readsOutput).setParallelism(1)
    env.execute()
  }

  def hashGroups(hashMax: Int, hsType: String): Option[Array[Int => Int]] = {
    hsType match {
      case "mod" => Some(Array(_ % hashMax))
      case _ => None
    }
  }

  def windowGroups(windowType: String, params: Seq[String]): Option[WindowRanges] = {
    windowType match {
      case "slidingRanges" =>
        Try(new SlidingRanges(dateStrToTimeStamp(params(0)), dateStrToTimeStamp(params(1)),
          params(2).toInt, params(3).toInt)).toOption
      case _ => None
    }
  }

  def main(args: Array[String]): Unit = {
    assert(args.length >= 1, "Usage: PROG_NAME CONFIG_FILENAME")
    assert(new java.io.File(args(0)).exists(), "File " + args(0) + " does not exist!")
    val config = ConfigFactory.parseFile(new java.io.File(args(0)))

    // TODO: try with hashing,
    // and with full BitSet (_ % 179922) (BloomFilters size would be: 2 GB + overhead)

    val logDirOpt = Try(config.getString("logDir")).toOption
    val wordsFilename = config.getString("wordsFilename")
    val tweetsFilename = config.getString("tweetsFilename")
    val readsHost = config.getString("readsHost")
    val readsPort = config.getInt("readsPort")
    val readsOutput = config.getString("readsOutput")
    val hashMax = config.getInt("hashMax")
    val hsType = config.getString("hsType")
    val numParts = config.getInt("numParts")
    val windowed = Try(config.getBoolean("windowed")).getOrElse(false)
    val windowType = Try(config.getString("windowType")).getOrElse("slidingRanges")
    val windowParams = Try(config.getStringList("windowParams")).map(_.asScala).getOrElse(Seq())

    val wP = config.getStringList("windowParams")

    logDirOpt.foreach(new java.io.File(_).isDirectory)
    val logFileOpt = logDirOpt.map(_ + "/" + java.time.LocalDateTime.now().toString + ".log")

    val hs = hashGroups(hashMax, hsType).get
    val windowRangesOpt = if (windowed) windowGroups(windowType, windowParams) else None

    if (windowRangesOpt.nonEmpty) {
      windowedCombOccs(wordsFilename, tweetsFilename, readsHost, readsPort, readsOutput,
        hashMax, hs, numParts, windowRangesOpt.get)
    } else {
      twitterCombOccs(wordsFilename, tweetsFilename, readsHost, readsPort, readsOutput,
        hashMax, hs, numParts)
    }
  }
}
