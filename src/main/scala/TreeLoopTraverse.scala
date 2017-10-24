import java.io.File

import scala.collection.mutable

object TreeLoopTraverse {

  def main(args: Array[String]): Unit = {
    traverse(".")
  }

  def traverse(path: String) = {
    def output(level: Int, file: File): Unit = {
      println("\t" * level + file.getName)
    }

    val stack = mutable.Stack[(Int, File)]((0, new File(path)))

    while (stack.nonEmpty) {
      val (level, file) = stack.pop()
      output(level, file)

      if (file.isDirectory) {
        val children = file.listFiles()
        children.reverse.map(child => stack.push((level + 1, child)))
      }
    }
  }
}
