package hotelbookingdemand.jobs

import org.rogach.scallop._


class Parameters(arguments: Seq[String]) extends ScallopConf(arguments) {
  val partitions = opt[Int](required = true)
  val input = opt[String](required = true)
  val out = opt[String](required = true)
  verify()

  override def toString() : String = {
    return "[partitions : " + partitions + ", input : "+ input +", out : "+ out + "]";
  }

}
