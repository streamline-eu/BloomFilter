package hu.sztaki.ilab.sketches

abstract class WindowRanges extends Serializable {
  // Returns true if t time is part of at least one window
  def validateTime(t: Long): Boolean

  // Returns the ids of ranges (windows) that contain t time
  def getRangeIds(t: Long): Seq[Int]

  // Returns the range id for a start time only if start time is exactly the start of a range
  def getRangeId(tStart: Long): Option[Int]

  // Returns the start and end time corresponding to the given range id
  def getInterval(id: Int): Seq[Long]
}
