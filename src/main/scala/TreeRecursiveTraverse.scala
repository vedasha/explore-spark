import java.io.File

object TreeRecursiveTraverse {

  def main(args: Array[String]): Unit = {
    traverse(".")
  }

  def traverse(path: String) = {
    def output(level: Int, file: File): Unit = {
      println("\t" * level + file.getName)
    }

    def traverse0(level: Int, file: File): Unit = {
      output(level, file)

      if (file.isDirectory) {
        val children = file.listFiles()
        for (child <- children) {
          traverse0(level + 1, child)
        }
      }

    }

    traverse0(0, new File(path))
  }
}
