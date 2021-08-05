package lastfm.jobs

import org.rogach.scallop.ScallopConf


class MostPopularSongsParameters(arguments: Seq[String]) extends ScallopConf(arguments) {

  val input = opt[String](required = true)
  val master = opt[String](required = true)
  val maxNumberOfSongsToReturn = opt[Int](required = true)


  verify()

  override def toString() : String = {
    return "[input : " + input + " master: " + master+ " maxNumberOfSongsToReturn: " + maxNumberOfSongsToReturn+ "]";
  }
}