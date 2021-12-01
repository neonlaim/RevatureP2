import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Hive {
  def main(args:Array[String]) : Unit = {
    connect()
    showData("1")
  }

  private var spark:SparkSession = _

  def connect() : Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    spark = SparkSession
      .builder()
      .appName("NFL DATA")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("DROP table IF EXISTS nfl_data")
    spark.sql("CREATE table IF NOT exists nfl_data(GameId int, GameDate Date, OffenseTeam String," +
      "DefenseTeam String, Description String,SeasonYear int, Yards int, Formation String, IsRush int," +
      "IsPass int, IsSack int, IsPenalty int, RushDirection String, PenaltyYards int)" +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'")

    spark.sql("Load data Local Inpath 'input/nfl_data2.csv' into table nfl_data")
    //    spark.sql("select * from nfl_data ").show(100)
  }



  def showData(choice:String) : Unit = {
    choice match {
      case "1" => spark.sql("SELECT count(isSack) as Total_Sacks " +
        "FROM nfl_data WHERE isSack = 1").show()
      case "2" => spark.sql("SELECT sum(yards) as Total_Rushing_Yards " +
        "FROM nfl_data").show()
      case "3" =>  spark.sql("SELECT sum(penaltyYards) as Total_Penalty_Yards " +
        "FROM nfl_data").show()
      case "4" => spark.sql("SELECT count(isRush) as Run_Plays_Right_Guard " +
        "FROM nfl_data WHERE isRush = 1 AND rushDirection = 'RIGHT GUARD'").show()
      case "5" => spark.sql("SELECT count(formation) as Total_Shotgun_Plays " +
        "FROM nfl_Data WHERE formation = 'SHOTGUN'").show()
      case "6" => spark.sql("SELECT ROUND(m.count/r.count,1) as YPR FROM " +
        "(SELECT count(isRush) count FROM nfl_data WHERE isRush = 1 AND OffenseTeam = 'SF') r, " +
        "(SELECT sum(yards) count FROM nfl_data WHERE isRush = 1 AND OffenseTeam = 'SF') m ").show()
      case "7" => spark.sql("select * from nfl_data ").show(50)
      case _ => println("No Results")
    }
  }


}

