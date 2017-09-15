package hu.sztaki.ilab.sketches

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.{immutable, mutable}
import scala.util.Try

class WindowedBloomCombinedOccurrences
(words: List[String], hashMax: Int, hs: Array[Int => Int], numParts: Int,
 windowRanges: WindowRanges) extends Serializable {
  // Tweet id, time, text
  type Tweet = (Int, Long, String)
  // Tweet id hash, time, text, partition
  type Write = (Int, Long, String, Int)
  // Word, window start time
  type RawRead = (String, Option[Long])
  // Word id, window id, partition
  type Read = (Option[Int], Option[Int], Int)
  // Either Write or Read
  type WriteOrRead = Either[Write, Read]
  // Word id, window id and its combined occurrences counts ((word id -> count) map)
  type Result = (Option[Int], Option[Int], immutable.HashMap[Int, Int])
  // Word, window id and its combined occurrences counts ((word -> count) map)
  type StringResult = (Option[String], Option[Int], immutable.HashMap[String, Int])

  class BloomStorePart {
    val bloom: mutable.HashMap[Int, mutable.HashMap[Int, mutable.HashMap[Int, mutable.BitSet]]] =
      mutable.HashMap()

    // Adds hash into the BitSet corresponding to part, window, word
    private def add(hash: Int, part: Int, window: Int, word: Int) = {
      bloom.get(part) match {
        case Some(wwbm) => wwbm.get(window) match {
          case Some(wbm) => wbm.get(word) match {
            case Some(bm) => wbm.update(word, bm + hash)
            case None => wbm += ((word, mutable.BitSet(hash)))
          }
          case None => wwbm += ((window, mutable.HashMap((word, mutable.BitSet(hash)))))
        }
        case None => bloom += ((part,
          mutable.HashMap((window, mutable.HashMap((word, mutable.BitSet(hash)))))))
      }
    }

    // Adds for all windows and words
    def add(hash: Int, part: Int, windows: Seq[Int], words: Seq[Int]): Unit =
      windows.foreach(window => words.foreach(word => add(hash, part, window, word)))

    // For a given part and window, the intersection sizes of word with all other words
    def getIntWithAll(part: Int, window: Int, word: Int): immutable.HashMap[Int, Int] = {
      if (!bloom.contains(part) || !bloom(part).contains(window)
        || !bloom(part)(window).contains(word)) { immutable.HashMap[Int, Int]()
      } else {
        val bWords = bloom(part)(window)
        immutable.HashMap(bWords.map(b => (b._1, (b._2 & bWords(word)).size))
          .filter(_._2 > 0).toSeq: _*)
      }
    }

    override def toString: String = bloom.toString
  }

  // Contains integer word indices
  private val wordIds: immutable.Map[String, Int] = words.zipWithIndex.toMap

  private def minHash(part: Int) = (part * hashMax + numParts - 1) / numParts

  /**
    * Outputs a list of Writes for every hash function
    */
  class BloomWriter extends FlatMapFunction[Tweet, Write] {
    override def flatMap(tweet: Tweet, hashes: Collector[Write]): Unit = {
      val (id, time, text) = tweet
      if (windowRanges.validateTime(time))
        hs.map(h => (h(id), time, text, h(id) * numParts / hashMax)).foreach(hashes.collect)
      // TODO: maybe log error message if validateTime fails
    }
  }

  /**
    * Broadcasts word id to all partitions once for every partition only if word is in words list
    */
  class BloomReader extends FlatMapFunction[RawRead, Read] {
    override def flatMap(read: RawRead, hashes: Collector[Read]): Unit = {
      val (word, startTime) = read
      val windowOpt = startTime.flatMap(windowRanges.getRangeId)
      val wordOpt = Try(wordIds(word)).toOption
      Range(0, numParts).map(part => (wordOpt, windowOpt, part)).foreach(hashes.collect)
    }
  }

  /**
    * Outputs a list of WriteOrReads for every hash function
    */
  class BloomWriterReader extends FlatMapFunction[Either[Tweet, RawRead], WriteOrRead] {
    override def flatMap(value: Either[Tweet, RawRead],
                         hashes: Collector[WriteOrRead]): Unit = {
      value match {
        case Left((id, time, text)) =>
          if (windowRanges.validateTime(time))
            hs.map(h => Left((h(id), time, text, h(id) * numParts / hashMax)))
              .foreach(hashes.collect)
          // TODO: maybe log error message if validateTime fails
        case Right((word, startTime)) =>
          val windowOpt = startTime.flatMap(windowRanges.getRangeId)
          val wordOpt = Try(wordIds(word)).toOption
          Range(0, numParts).map(part => Right((wordOpt, windowOpt, part))).foreach(hashes.collect)
      }
    }
  }

  // Tweet to words definition: regex (\p{L}|\p{N})+
  def tweetToWords(tweetStr: String): Seq[String] =
    """(\p{L}|\p{N})+""".r().findAllIn(tweetStr).filter(wordIds.contains).map(_.toLowerCase).toSeq

  private def bloomWordsFold(bloomStore: BloomStorePart, write: Write): BloomStorePart = {
    val (hash, time, text, part) = write
    val windows = windowRanges.getRangeIds(time)
    val words = tweetToWords(text).map(wordIds(_))
    bloomStore.add(hash - minHash(part), part, windows, words)
    bloomStore
  }

  private def bloomFiltersWrite(bloomsState: ValueState[BloomStorePart], write: Write) = {
    bloomsState.update(bloomWordsFold(bloomsState.value(), write))
  }

  private def bloomFiltersRead(bloomsState: ValueState[BloomStorePart], read: Read,
                               results: Collector[Result]) = {
    val (wordOpt, windowOpt, part) = read
    val intWithAll = if (wordOpt.isEmpty || windowOpt.isEmpty) immutable.HashMap[Int, Int]()
    else bloomsState.value().getIntWithAll(part, windowOpt.get, wordOpt.get)
    results.collect((wordOpt, windowOpt, intWithAll))
  }

  /**
    * BloomFilter, receives individual write and read operations, and processes them
    */
  class BloomFilters extends RichCoFlatMapFunction[Write, Read, Result] {
    var bloomsState: ValueState[BloomStorePart] = _

    override def flatMap1(write: Write, results: Collector[Result]): Unit =
      bloomFiltersWrite(bloomsState, write)

    override def flatMap2(read: Read, results: Collector[Result]): Unit =
      bloomFiltersRead(bloomsState, read, results)

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[BloomStorePart](
        "BloomFilterState", classOf[BloomStorePart], new BloomStorePart)
      bloomsState = getRuntimeContext.getState(descriptor)
    }
  }

  /**
    * BloomFilter, receives a stream of write or read operations, and processes them
    */
  class BloomFiltersEither extends RichFlatMapFunction[WriteOrRead, Result] {
    var bloomsState: ValueState[BloomStorePart] = _

    override def flatMap(writeOrRead: WriteOrRead, results: Collector[Result]): Unit = {
      writeOrRead match {
        case Left(write) => bloomFiltersWrite(bloomsState, write)
        case Right(read) => bloomFiltersRead(bloomsState, read, results)
      }
    }

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[BloomStorePart](
        "BloomFiltersState", classOf[BloomStorePart], new BloomStorePart)
      bloomsState = getRuntimeContext.getState(descriptor)
    }
  }

  /**
    * Aggregates BloomFilters read operation results into a StringResult
    */
  class CombinedCountAggregator extends RichFlatMapFunction[Result, StringResult] {
    var combinedCountsState: ValueState[(Int, Result)] = _

    override def flatMap(input: Result, outputs: Collector[StringResult]): Unit = {
      var combinedCounts = combinedCountsState.value()
      if (combinedCounts._1 == 0) {
        combinedCountsState.update(1, input)
      } else {
        val counts = combinedCounts._2._3.merged(input._3)
          { case ((k, v1), (_, v2)) => (k, v1 + v2) }
        if (combinedCounts._1 + 1 < numParts) {
          combinedCountsState.update((combinedCounts._1 + 1, (input._1, input._2, counts)))
        } else {
          outputs.collect((input._1.map(words(_)), input._2, counts.map(c => (words(c._1), c._2))))
          combinedCountsState.update((0, (None, None, immutable.HashMap[Int, Int]())))
        }
        // TODO: use a unique id for every Read as part of the key, so Reads don't mix
      }
    }

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[(Int, Result)](
        "CombinedCountAggregateState", classOf[(Int, Result)],
        (0, (None, None, immutable.HashMap[Int, Int]())))
      combinedCountsState = getRuntimeContext.getState(descriptor)
    }
  }

  // Appliers:

  def applyCombOccs(writeStream: DataStream[Write], readStream: DataStream[Read])
  : DataStream[StringResult] = {
    writeStream.connect(readStream)
      .keyBy(write => write._4, read => read._3) // key by part
      .flatMap(new BloomFilters)
      .keyBy(result => (result._1, result._2)) // key by (wordId, window start time)
      .flatMap(new CombinedCountAggregator)
  }

  def applyCombOccsRaw(tweetStream: DataStream[Tweet],
                       wordStream: DataStream[RawRead]): DataStream[StringResult] =
    applyCombOccs(tweetStream.flatMap(new BloomWriter), wordStream.flatMap(new BloomReader))

}
