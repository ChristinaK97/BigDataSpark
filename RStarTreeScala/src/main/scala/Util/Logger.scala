package Util

import java.io.PrintWriter

class Logger {

  val pr: PrintWriter = new PrintWriter("log.txt")

  def info(string: String):Unit = {
    pr.println(string)
  }

  def close(): Unit =
    pr.close()

}
