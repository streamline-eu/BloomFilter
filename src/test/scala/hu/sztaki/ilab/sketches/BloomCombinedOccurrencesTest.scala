package hu.sztaki.ilab.sketches

//import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test

import scala.collection.immutable
import scala.collection.immutable.HashMap
import scala.util.Try

class BloomCombinedOccurrencesTest extends StreamingMultipleProgramsTestBase with Serializable {

  /**
    * Helper Sink class for stream testing.
    * @param assertElem function testing stream element
    * @param assertNum function testing the number of stream elements
    * @tparam T stream type
    */
  class TestSink[T](assertElem: T => Unit, assertNum: Int => Unit) extends RichSinkFunction[T] {
    var numResults = 0
    override def invoke(result: T): Unit = { assertElem(result); numResults += 1 }
    override def close(): Unit = assertNum(numResults)
  }

  @Test
  def simpleMod10Hashes(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Hash functions
    val hashMax = 10
    val hs: Array[Int => Int] = Array(_ % hashMax, _ % hashMax)

    val words = List[String]("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
    val wordIds: immutable.Map[String, Int] = words.zipWithIndex.toMap
    val bloomCombOccs = new BloomCombinedOccurrences(words, hashMax, hs, 4)

    val tweetList: List[(Int, String)] = Range(0, 9).toList zip List[String](
      "a b c d e f g h i",
      "a c d e z z z",
      "e h g j p p p p p p p",
      "a c e j l",
      "a e j",
      "f g h i l",
      "e c a",
      "a e",
      "",
      "b c d e l"
    )
    val readList = List[String](
      "a", "j"
    )
    val inputList: List[Either[(Int, String), String]] =
      tweetList.map(Left(_)) ++ readList.map(Right(_))

    val expectedResults = immutable.HashMap(
      (wordIds("a"), List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j").map(wordIds)
          .zip(List(6,1,4,2,6,1,1,1,1,2)).toMap),
      (wordIds("j"), List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j").map(wordIds)
          .zip(List(2,0,1,0,3,0,1,1,0,3)).toMap)
    )

    env.fromCollection(inputList)
      .flatMap(new bloomCombOccs.BloomWriterReader).setParallelism(1)
      .keyBy(writeOrRead => writeOrRead.fold(write => write._2, read => read._2))
      .flatMap(new bloomCombOccs.BloomFiltersEither).setParallelism(1)
      .keyBy(_._1)
      .flatMap(new bloomCombOccs.CombinedCountAggregator)
      .addSink(new TestSink[(String, HashMap[String, Int])](
        elem => elem._2.foreach(e =>
          assert(expectedResults(wordIds(elem._1))(wordIds(e._1)) == e._2)),
        num => assert(expectedResults.size == num)
      )).setParallelism(1)

    env.execute("BloomFilterTest - simpleMod10Hashes")
  }

  @Test
  def windowedMod10Hashes(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Hash functions
    val hashMax = 10
    val hs: Array[Int => Int] = Array(_ % hashMax, _ % hashMax)

    val words = List[String]("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
    val wordIds: immutable.Map[String, Int] = words.zipWithIndex.toMap
    val slidingRanges = new SlidingRanges(0, 10, 5, 5)
    val bloomCombOccs = new WindowedBloomCombinedOccurrences(words, hashMax, hs, 4, slidingRanges)

    val tweetList: List[(Int, Long, String)] = Range(0, 10).toList zip List[String](
      "a b c d e f g h i",
      "a c d e z z z",
      "e h g j p p p p p p p",
      "a c e j l",
      "a e j",
      "f g h i l",
      "e c a",
      "a e",
      "",
      "b c d e l"
    ) map (t => (t._1, t._1.toLong, t._2))
    val readList: List[(String, Option[Long])] = List[(String, Long)](
      ("a", 0),
      ("j", 0),
      ("a", 5),
      ("j", 5)
    ).map(r => (r._1, Some(r._2)))

    val inputList: List[Either[(Int, Long, String), (String, Option[Long])]] =
      tweetList.map(Left(_)) ++ readList.map(Right(_))

    val expectedResults = immutable.HashMap(
      (wordIds("a"), immutable.HashMap(
        (0, List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j").map(wordIds)
        .zip(List(4,1,3,2,4,1,1,1,1,2)).toMap),
        (1, List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j").map(wordIds)
          .zip(List(2,0,1,0,2,0,0,0,0,0)).toMap)
      )), (wordIds("j"), immutable.HashMap(
        (0, List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j").map(wordIds)
        .zip(List(2,0,1,0,3,0,1,1,0,3)).toMap),
        (1, List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j").map(wordIds)
          .zip(List(0,0,0,0,0,0,0,0,0,0)).toMap)
      ))
    )

    env.fromCollection(inputList)
      .flatMap(new bloomCombOccs.BloomWriterReader).setParallelism(1)
      .keyBy(writeOrRead => writeOrRead.fold(write => write._4, read => read._3))
      .flatMap(new bloomCombOccs.BloomFiltersEither).setParallelism(1)
      .keyBy(_._1)
      .flatMap(new bloomCombOccs.CombinedCountAggregator)
      .addSink(new TestSink[(Option[String], Option[Int], HashMap[String, Int])](
        elem => elem._3.foreach(e => {Try(
          assert(expectedResults(wordIds(elem._1.get))(elem._2.get)(wordIds(e._1)) == e._2) )}),
        num => {
          assert(expectedResults.foldLeft(0)((a, e) => a + e._2.size) == num)
        }
      )).setParallelism(1)

    env.execute("BloomFilterTest - simpleMod10Hashes")
  }

}
