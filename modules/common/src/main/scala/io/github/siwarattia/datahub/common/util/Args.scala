package io.github.siwarattia.datahub.common.util

/** Minimal --key value argument parser shared by all jobs. */
object Args {

  def parse(args: Array[String]): Map[String, String] =
    args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") => key.stripPrefix("--") -> value
    }.toMap
}
