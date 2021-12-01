import java.sql.{Connection, DriverManager}


object UserDB {

  private var connection:Connection = _

  def connect(): Unit = {
  }

  def closeConnection() : Unit = {
    connection.close()
  }
}

