package lastfm.jobs

import org.rogach.scallop._


class LongestSessionParameters(arguments: Seq[String]) extends ScallopConf(arguments) {

  val input = opt[String](required = true)
  val master = opt[String](required = true)
  val maxDelataInMinutesForSession = opt[Int](required = true)
  val numberOfTopLongestSessionsToReturn = opt[Int](required = true)

  verify()

  override def toString() : String = {
    return "[input : " + input + " master: " + master+ " maxDelataInMinutesForSession: " + maxDelataInMinutesForSession + "]";
  }

}
