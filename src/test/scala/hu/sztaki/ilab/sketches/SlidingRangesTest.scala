package hu.sztaki.ilab.sketches

import org.junit.Test

class SlidingRangesTest {

  @Test
  def slidingExample1(): Unit = {
    val sr = new SlidingRanges(0, 20, 10, 5)
    Seq(0, 5, 10, 15, 20, 25, -1, 1, 2, 3, 4, 6, 9, 11, 14, 16, 19, 21).zipAll(
      Seq(0, 1, 2, 3).map(Some(_)), -1, None).foreach(elem => {
      assert(sr.getRangeId(elem._1) == elem._2)
      assert(sr.validateTime(elem._1) == (0 <= elem._1 && elem._1 < 25))
    })

    Range(-100, 125).zip(Seq.fill(100)(Seq()) ++ Seq.fill(5)(Seq(0)) ++ Seq.fill(5)(Seq(0,1)) ++
      Seq.fill(5)(Seq(1,2)) ++ Seq.fill(5)(Seq(2,3)) ++ Seq.fill(5)(Seq(3)) ++ Seq.fill(100)(Seq())
    ).foreach(elem => {
      assert(sr.getRangeIds(elem._1) == elem._2)
    })

    Range(-100, 104).zip(Seq.fill(100)(Seq()) ++
      Seq(Seq(0, 10), Seq(5, 15), Seq(10, 20), Seq(15, 25)) ++ Seq.fill(100)(Seq())
    ).foreach(elem => {
      assert(sr.getInterval(elem._1) == elem._2)
    })
  }

  @Test
  def slidingExample2(): Unit = {
    val sr = new SlidingRanges(500, 520, 100, 15)
    Seq(500, 515, 499, 501, 502, 510, 514, 516, 517, 518, 519, 520, 521, 529, 530, 531, 0).zipAll(
      Seq(0, 1).map(Some(_)), -1, None).foreach(elem => {
      assert(sr.getRangeId(elem._1) == elem._2)
      assert(sr.validateTime(elem._1) == (500 <= elem._1 && elem._1 < 615))
    })

    Range(-100, 715).zip(Seq.fill(600)(Seq()) ++ Seq.fill(15)(Seq(0)) ++ Seq.fill(85)(Seq(0, 1))
      ++ Seq.fill(15)(Seq(1)) ++ Seq.fill(100)(Seq())
    ).foreach(elem => {
      assert(sr.getRangeIds(elem._1) == elem._2)
    })

    Range(-100, 102).zip(Seq.fill(100)(Seq()) ++ Seq(Seq(500, 600)) ++ Seq(Seq(515, 615))
      ++ Seq.fill(100)(Seq())
    ).foreach(elem => {
      assert(sr.getInterval(elem._1) == elem._2)
    })
  }

}
