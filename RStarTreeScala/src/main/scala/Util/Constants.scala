package Util

import java.io.File
import java.nio.file.Paths

object Constants {
  var N: Int = -1
  val sizeOfDouble: Int = 8
  val sizeOfInt: Int = 4
  val ONE_MM: Double = 0.00001

  //8192 // 2048 // // 32768 //8192 //2048 // 16384// //bytes
  val SMALL_SET_LIMIT = 8192
  val LARGE_SET_LIMIT = 16384
  val UP_LIMIT: Int = SMALL_SET_LIMIT

  val RESET_TREES: Boolean = true
  val DEBUG: Boolean = false
  val DEBUG_SKY = false
  val DEBUG_TOPK = false
}


object DirectoryDeletion {
  def deleteDirectory(directoryPath: String): Unit = {
    val directory = new File(Paths.get(directoryPath).toString)

    val files = Option(directory.listFiles())

    files.foreach { fileList =>
      fileList.foreach { file =>
        if (file.isDirectory)
          deleteDirectory(file.getAbsolutePath)
        else if (!file.delete())
            System.err.println(s"Failed to delete file: ${file.getAbsolutePath}")
      }
    }
    if (!directory.delete()) {
      System.err.println(s"Failed to delete directory: ${directory.getAbsolutePath}")
    }
  }
}
