package hu.sztaki.ilab.sketches

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

import scala.collection.{immutable, mutable}
import scala.util.Try

class BloomCombinedOccurrences
(words: List[String], hashMax: Int, hs: Array[Int => Int], numParts: Int) extends Serializable {
  // Tweet id, text
  type Tweet = (Int, String)
  // Tweet id hash, partition, text
  type Write = (Int, Int, String)
  // Word id, partition
  type Read = (Int, Int)
  // Either Write or Read
  type WriteOrRead = Either[Write, Read]
  // Container for some numbered partitions of the Bloom BitSet
  type BloomParts = immutable.HashMap[Int, mutable.BitSet]
  // Container for BloomParts for all words
  type BloomWordsParts = immutable.HashMap[Int, BloomParts]
  // Word id and its combined occurrences counts ((word id -> count) map)
  type Result = (Int, immutable.HashMap[Int, Int])
  // Word and its combined occurrences counts ((word -> count) map)
  type StringResult = (String, immutable.HashMap[String, Int])

  // Contains integer word indices
  private val wordIds: immutable.Map[String, Int] = words.zipWithIndex.toMap

  private def minHash(part: Int) = (part * hashMax + numParts - 1) / numParts

  private def getBitSet(bloom: BloomParts, part: Int): mutable.BitSet =
    bloom.getOrElse(part, new mutable.BitSet)

  private def addToBitSet(bloom: BloomParts, hash: Int, part: Int) =
    bloom + ((part, getBitSet(bloom, part) + hash))

  private def getBloomParts(blooms: BloomWordsParts, word: Int) =
    blooms.getOrElse(word, new BloomParts)

  private def addToBloomParts(blooms: BloomWordsParts, word: Int, hash: Int, part: Int) =
    blooms + ((word, addToBitSet(getBloomParts(blooms, word), hash, part)))

  private def intersect(bloom1: BloomParts, bloom2: BloomParts): BloomParts = bloom1
    .map(bp => (bp._1, bloom2.get(bp._1).map(_ & bp._2).getOrElse(bp._2)))
    .filter(_._2.nonEmpty)

  private def getSize(bloom: BloomParts): Int =
    bloom.foldLeft(0)((acc, b) => acc + b._2.size)

  /**
    * Outputs a list of Writes for every hash function
    */
  class BloomWriter extends FlatMapFunction[Tweet, Write] {
    override def flatMap(value: Tweet, hashes: Collector[Write]): Unit = {
      val (id, text) = value
      hs.map(h => (h(id), h(id) * numParts / hashMax, text)).foreach(hashes.collect)
    }
  }

  /**
    * Broadcasts word id to all partitions once for every partition only if word is in words list
    */
  class BloomReader extends FlatMapFunction[String, Read] {
    override def flatMap(value: String, hashes: Collector[Read]): Unit = {
      Try(Range(0, numParts).map(part => (wordIds(value), part)).foreach(hashes.collect))
      // TODO: maybe log error message if .failure
    }
  }

  /**
    * Outputs a list of WriteOrReads for every hash function
    */
  class BloomWriterReader extends FlatMapFunction[Either[Tweet, String], WriteOrRead] {
    override def flatMap(value: Either[Tweet, String],
                         hashes: Collector[WriteOrRead]): Unit = {
      value match {
        case Left((id, text)) =>
          hs.map(h => Left((h(id), h(id) * numParts / hashMax, text))).foreach(hashes.collect)
        case Right(read) =>
          Range(0, numParts).map(part => Right((wordIds(read), part))).foreach(hashes.collect)
      }
    }
  }

  // Tweet to words definition: regex (\p{L}|\p{N})+
  def tweetToWords(tweetStr: String): List[String] =
    """(\p{L}|\p{N})+""".r().findAllIn(tweetStr).filter(wordIds.contains).map(_.toLowerCase).toList

  private def bloomWordsFold(blooms: BloomWordsParts, write: Write): BloomWordsParts = {
    val (hash, part, text) = write
    tweetToWords(text).map(wordIds(_)).foldLeft(blooms)(
      (blooms, wordId) => addToBloomParts(blooms, wordId, hash - minHash(part), part))
  }

  private def bloomMerge(bloom1: BloomParts, bloom2: BloomParts) =
    bloom1.merged(bloom2)({ case ((k, v1), (_, v2)) => (k, v1 ++ v2) })

  private def bloomWordsMerge(blooms1: BloomWordsParts, blooms2: BloomWordsParts) =
    blooms1.merged(blooms2)({ case ((k, v1), (_, v2)) => (k, bloomMerge(v1, v2)) })

  private def bloomFiltersWrite(bloomsState: ValueState[BloomWordsParts], write: Write) =
    bloomsState.update(bloomWordsFold(bloomsState.value(), write))

  private def bloomFiltersRead(bloomsState: ValueState[BloomWordsParts], read: Read,
                               results: Collector[Result]) = {
    val blooms = bloomsState.value()
    val wordOccurrences = if (!blooms.contains(read._1)) immutable.HashMap[Int, Int]()
    else blooms.map(b => (b._1, getSize(intersect(b._2, blooms(read._1)))))
      .filter(_._2 > 0)
    results.collect(read._1, wordOccurrences)
  }

  /**
    * BloomFilter, receives individual write and read operations, and processes them
    */
  class BloomFilters extends RichCoFlatMapFunction[Write, Read, Result] {
    var bloomsState: ValueState[BloomWordsParts] = _

    override def flatMap1(write: Write, results: Collector[Result]): Unit =
      bloomFiltersWrite(bloomsState, write)

    override def flatMap2(read: Read, results: Collector[Result]): Unit =
      bloomFiltersRead(bloomsState, read, results)

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[BloomWordsParts](
        "BloomFilterState", classOf[BloomWordsParts], new BloomWordsParts)
      bloomsState = getRuntimeContext.getState(descriptor)
    }
  }

  /**
    * BloomFilter, receives a stream of write or read operations, and processes them
    */
  class BloomFiltersEither extends RichFlatMapFunction[WriteOrRead, Result] {
    var bloomsState: ValueState[BloomWordsParts] = _

    override def flatMap(writeOrRead: WriteOrRead, results: Collector[Result]): Unit = {
      writeOrRead match {
        case Left(write) => bloomFiltersWrite(bloomsState, write)
        case Right(read) => bloomFiltersRead(bloomsState, read, results)
      }
    }

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[BloomWordsParts](
        "BloomFiltersState", classOf[BloomWordsParts], new BloomWordsParts)
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
        val counts = combinedCounts._2._2.merged(input._2)({ case ((k, v1), (_, v2)) => (k, v1 + v2) })
        if (combinedCounts._1 + 1 < numParts) {
          combinedCountsState.update((combinedCounts._1 + 1, (input._1, counts)))
        } else {
          outputs.collect((words(input._1), counts.map(c => (words(c._1), c._2))))
          combinedCountsState.update(0, (0, immutable.HashMap[Int, Int]()))
        }
      }
    }

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[(Int, Result)](
        "CombinedCountAggregateState", classOf[(Int, Result)],
        (0, (0, immutable.HashMap[Int, Int]())))
      combinedCountsState = getRuntimeContext.getState(descriptor)
    }
  }

  // Appliers:

  def applyCombOccs(writeStream: DataStream[Write], readStream: DataStream[Read])
  : DataStream[StringResult] = {
    writeStream.connect(readStream)
      .keyBy(write => write._2, read => read._2)
      .flatMap(new BloomFilters)
      .keyBy(result => result._1)
      .flatMap(new CombinedCountAggregator)
  }

  def applyCombOccsRaw(tweetStream: DataStream[(Int, String)],
                       wordStream: DataStream[String]): DataStream[StringResult] =
    applyCombOccs(tweetStream.flatMap(new BloomWriter), wordStream.flatMap(new BloomReader))

}