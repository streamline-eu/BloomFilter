package hu.sztaki.ilab.sketches

/** Represents sliding windows
  *
  * @param start the time from which windows start
  * @param end the time from which windows no longer start (last window starts before end)
  * @param size the size of the window
  * @param slide the time difference between the window starting times
  */
class SlidingRanges(start: Long, end: Long, size: Long, slide: Long) extends WindowRanges {
  private val numRanges = (1 + (end - start - 1) / slide).toInt
  private def validId(id: Int) = 0 until numRanges contains id

  def validateTime(t: Long): Boolean =
    (t >= start) && (t < start + size + (numRanges - 1)*slide)

  def getRangeIds(t: Long): Seq[Int] = {
    val d = t - start
    if (d < 0) Seq() // (the else-part works correctly only for d >= 0)
    else (((d - size + slide) / slide) to (d / slide)).map(_.toInt).filter(validId)
  }

  def getRangeId(tStart: Long): Option[Int] = {
    val d = tStart - start
    if ((d % slide != 0) || !validId((d / slide).toInt)) None
    else Some((d / slide).toInt)
  }

  def getInterval(id: Int): Seq[Long] =
    if (validId(id)) Seq(start + slide * id, start + slide * id + size) else Seq()
}
