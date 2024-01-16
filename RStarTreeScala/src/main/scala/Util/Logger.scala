package Util

import java.io.PrintWriter
import java.nio.file.{Path, Paths}

class Logger(PARTITION_PATH: Path) {

  val pr: PrintWriter = new PrintWriter(Paths.get(s"$PARTITION_PATH/log.txt").toString)

  def info(string: String):Unit = {
    pr.println(string)
  }

  def close(): Unit =
    pr.close()

}
