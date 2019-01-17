import org.apache.spark.sql.SQLContext
import org.json4s._
import org.json4s.jackson.JsonMethods._


object DocGenJSON4S {


  def gen(jsonMeta: String, sQLContext: SQLContext): Unit = {
    val json = parse(jsonMeta)

    val databases = (json \ "databases").children

    // Optional JString, can not using JString if it could be JNothing
    class JStringOpt(default: String) {
      def unapply(e: Any) = e match {
        case d: JString => JString.unapply(d)
        case _ => Some(default)
      }
    }

    // default value if there is no value provided
    val jStringOptAnonymizer = new JStringOpt("no-anonymizer")


    val predefinedDatabaseFields = Set("database_name", "tables")

    for (database <- databases) {

      val JString(databaseName) = database \ "database_name"
      println(s"database_name = $databaseName" )

      sQLContext.sql(s"use $databaseName")

      for (JField(name, value) <- database if !predefinedDatabaseFields.contains(name)) {
        println(value)
      }

      val tables = (database \ "tables").children

      for (table <- tables) {
        val JString(tableName) = table \ "table_name"
        println(s"    table_name = $tableName")


        val columns = (table \ "columns").children
        for (column <- columns) {
          val JString(columnName) = column \ "column_name"
          val JString(columnDescription) = column \ "column_description"
          val jStringOptAnonymizer(anonymizer) = column \ "anonymizer"


          println(s"        column name = $columnName, description = $columnDescription, anonymizer = $anonymizer")
        }
      }

    }
  }

  def main(args: Array[String]): Unit = {

  }
}
