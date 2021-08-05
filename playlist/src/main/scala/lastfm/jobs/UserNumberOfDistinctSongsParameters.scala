package lastfm.jobs

import org.rogach.scallop.ScallopConf



class UserNumberOfDistinctSongsParameters(arguments: Seq[String]) extends ScallopConf(arguments) {

  val input = opt[String](required = true)
  val master = opt[String](required = true)


  verify()

  override def toString() : String = {
    return "[input : " + input + " master: " + master+ "]";
  }
}