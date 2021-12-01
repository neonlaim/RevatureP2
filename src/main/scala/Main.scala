import java.beans.Statement
import java.sql.{Connection, DriverManager, SQLException}
import scala.io.StdIn
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.util.control.Breaks._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.{col, count, countDistinct, desc, when}
import CustomImplicits._


import java.security.MessageDigest
import scala.annotation.tailrec
import scala.sys.exit


object Main {

  def main(args:Array[String]): Unit = {
    var valid_cred = false
    var user = ""
    var pass = ""
    var encryptedpass = ""
    var privileges = ""
    var mainmenuselection = ""
    var mainmenucheck = false
    var optionmenuselection = ""
    var optionmenucheck = false
    var programexitcheck = false

    //CONNECT TO DATABASE TO GET USERS LOGIN INFO//
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = System.getenv("jdbc:mysql://localhost:3306/nfl")
    val username = System.getenv("root")
    val password = System.getenv("000000")

    var connection: Connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()

    //CONNECT TO SPARK AND CREATE A TABLE FOR SQL-LIKE QUERIES IN SPARK//
    //Hive.connect()
    //READS THE OLD(CURRENT) nfl_data2.csv TO CREATE A TABLE
    //COMMENTED OUT TO AVOID BREAKING THE CODE IF THE NEW FILE IS USED
    //UNCOMMENT IF THE TABLE CREATION HAS BEEN UPDATED TO INCLUDE THE NEW COLUMNS IN THE NEW FILE

    //INITIATE SPARK SESSION//
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")

    //P2 ADDITION////P2 ADDITION////P2 ADDITION////P2 ADDITION//
    //CREATE BROADCAST VARIABLE, DATAFRAME, DATASET, AND RDD TO USE THROUGHOUT THE PROGRAM//

    //DATAFRAME//
    val nfldf = spark.read.option("header","true").option("delimiter",",").option("inferSchema","true").csv("input/nfldata_updated.csv")
    val reparnfldf = nfldf.repartition(3)
    reparnfldf.persist(StorageLevel.MEMORY_AND_DISK)
    //DATASET//
    val nflds = spark.read.option("header","true").option("delimiter",",").option("inferSchema","true").csv("input/nfldata_updated.csv").as[NFL]
    val reparnflds = nflds.repartition(3)
    reparnflds.persist(StorageLevel.MEMORY_AND_DISK)
    //RDD//
    val nflrdd = nfldf.rdd
    val reparnflrdd = nflrdd.repartition(3)
    reparnflrdd.persist(StorageLevel.MEMORY_AND_DISK)
    //BROADCAST VARIABLE (DATAFRAME)//
    val broadcastData = spark.sparkContext.broadcast(reparnfldf)
    val broadcastDataNoRepar = spark.sparkContext.broadcast(nfldf)


    //P2 ADDITION////P2 ADDITION////P2 ADDITION////P2 ADDITION//

    //PROGRAM LOOP
    do {

      valid_cred = false
      user = ""
      pass = ""
      encryptedpass = ""
      mainmenuselection = ""
      mainmenucheck = false
      optionmenuselection = ""
      optionmenucheck = false
      programexitcheck = false

      //MAIN MENU//
      do {
        println("Welcome to the main menu, please make a selection")
        println("1) Sign In")
        println("2) Exit")
        print("> ")
        mainmenuselection = StdIn.readLine()
        println()

        mainmenuselection match {
          case "1" =>
            //USER SIGN IN//
            do {
              print("Enter your username: ")
              user = StdIn.readLine()
              print("Enter your password: ")
              pass = StdIn.readLine()
              encryptedpass = md5(pass)
              println()

              var resultSet = statement.executeQuery("SELECT * FROM users;")
              resultSet.next()

              breakable {
                do {
                  var check_user = resultSet.getString(2)
                  var check_pass = resultSet.getString(3)
                  privileges = resultSet.getString(4)

                  if (check_user == user && check_pass == encryptedpass) {
                    valid_cred = true
                    mainmenucheck = true
                    println("Success! Welcome '" + user + "' with '" + privileges + "' privileges")
                    println()
                    break
                  }

                } while (resultSet.next())

                if (valid_cred == false) {
                  println(Console.RED + "ERROR: INCORRECT USER OR PASSWORD, TRY AGAIN" + Console.RESET)
                  println()
                }

              }

            } while (!valid_cred)

          case "2" =>
            mainmenucheck = true
            programexitcheck = true

          case _ => println(Console.RED + "ERROR, UNEXPECTED COMMAND: select a valid command from the selection menu" + Console.RESET)

        }

      } while (!mainmenucheck)


      if (valid_cred) {

        //OPTION MENU
        do {
          println("What would you like to do today, " + user + "?")
          println("1) Most Popular Formations")
          println("2) Total Sacks")
          println("3) Fourth Down Success Rates")
          println("4) SF Yards Per Rush Direction")
          println("5) Penalty Yards by Team and Formation")
          println("6) Yards in Shotgun Formation")
          if(privileges == "admin"){
            println("7) Add user -ADMIN ONLY OPTION-")
            println("8) Delete user -ADMIN ONLY OPTION")
          }
          println("9) Back")
          print("> ")
          optionmenuselection = StdIn.readLine()
          println()

          if((optionmenuselection == "2" || optionmenuselection == "3") && privileges != "admin"){
            println(Console.YELLOW + "WARNING: ADMIN PRIVILEGE REQUIRED TO ACCESS THIS FEATURES" + Console.RESET)
            println()
            optionmenuselection = "thisdoesnothing"
          }

          optionmenuselection match {
            case "1" =>
              //QUERY 1
              //QUERY FORM: SELECT DISTINCT Formation, COUNT(Formation) OVER(Partition by Formation) AS Total_Times_Seen FROM nfl_data ORDER BY Total_Times_Seen DESC
              broadcastData.value.groupBy("Formation").count().sort(desc("count")).withColumnRenamed("count","Total_Times_Seen").show()

            case "2" =>
              //QUERY 2
              //QUERY FORM: SELECT count(isSack) as Total_Sacks FROM nfl_data WHERE isSack = 1
              nflds.groupBy("isSack").count().filter("isSack == 1").withColumnRenamed("count","Total_Sacks").show()

            case "3" =>
              //QUERY 3
              val fourthDownSuccess10 = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
                (broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1) && (broadcastDataNoRepar.value("Yards") >= broadcastDataNoRepar.value("ToGo")) && (broadcastDataNoRepar.value("ToGo") >= 9 ))
              val fourthDownCntSuccess10 = fourthDownSuccess10.agg(functions.count("Down")).first.getLong(0)


              val fourthDownSuccess8 = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
                (broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1) && (broadcastDataNoRepar.value("Yards") >= broadcastDataNoRepar.value("ToGo")) && ((broadcastDataNoRepar.value("ToGo") <= 8) && (broadcastDataNoRepar.value("ToGo") > 6)))
              val fourthDownCntSuccess8 = fourthDownSuccess8.agg(functions.count("Down")).first.getLong(0)

              val fourthDownSuccess6 = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
                ((broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1)) && (broadcastDataNoRepar.value("Yards") >= broadcastDataNoRepar.value("ToGo")) && ((broadcastDataNoRepar.value("ToGo") === 6) || (broadcastDataNoRepar.value("ToGo") === 5)))
              val fourthDownCntSuccess6 = fourthDownSuccess6.agg(functions.count("Down")).first.getLong(0)

              val fourthDownSuccess4 = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
                ((broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1)) && (broadcastDataNoRepar.value("Yards") >= broadcastDataNoRepar.value("ToGo")) && ((broadcastDataNoRepar.value("ToGo") === 4) || (broadcastDataNoRepar.value("ToGo") === 3)))
              val fourthDownCntSuccess4 = fourthDownSuccess4.agg(functions.count("Down")).first.getLong(0)

              val fourthDownSuccess2 = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
                ((broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1)) && (broadcastDataNoRepar.value("Yards") >= broadcastDataNoRepar.value("ToGo")) && ((broadcastDataNoRepar.value("ToGo") === 2) || (broadcastDataNoRepar.value("ToGo") === 1)))
              val fourthDownCntSuccess2 = fourthDownSuccess2.agg(functions.count("Down")).first.getLong(0)

              val fourthDownTotal = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
                (broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1))
              val fourthDownCntTotal = fourthDownTotal.agg(functions.count("Down")).first.getLong(0)

              val successRate10 = (fourthDownCntSuccess10.toDouble/fourthDownCntTotal.toDouble) *100.0
              val successRate8 = (fourthDownCntSuccess8.toDouble/fourthDownCntTotal.toDouble) *100.0
              val successRate6 = (fourthDownCntSuccess6.toDouble/fourthDownCntTotal.toDouble) *100.0
              val successRate4 = (fourthDownCntSuccess4.toDouble/fourthDownCntTotal.toDouble) *100.0
              val successRate2 = (fourthDownCntSuccess2.toDouble/fourthDownCntTotal.toDouble) *100.0

              println("4th Down Plays Success Rate 9 yards or more: " + f"$successRate10%1.2f" + "%")
              println("4th Down Plays Success Rate 7-8 yards: " + f"$successRate8%1.2f" + "%")
              println("4th Down Plays Success Rate 5-6 yards: " + f"$successRate6%1.2f" + "%")
              println("4th Down Plays Success Rate 3-4 yards: " + f"$successRate4%1.2f" + "%")
              println("4th Down Plays Success Rate 1-2 yards: " + f"$successRate2%1.2f" + "%")
              println()

            case "4" =>
              //QUERY 4
              val rdLeftEnd = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
                broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "LEFT END")
              val sumLeftEnd = rdLeftEnd.agg(functions.sum("yards")).first.get(0)


              //Right End Yards Sum
              val rdRightEnd = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
                broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "RIGHT END")
              val sumRightEnd = rdRightEnd.agg(functions.sum("yards")).first.get(0)


              //Left Guard Yards Sum
              val rdLeftGuard = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
                broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "LEFT GUARD")
              val sumLeftGuard = rdLeftGuard.agg(functions.sum("yards")).first.get(0)


              //Right Guard Yards Sum
              val rdRightGuard = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
                broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "RIGHT GUARD")
              val sumRightGuard = rdRightGuard.agg(functions.sum("yards")).first.get(0)


              //Left Tackle Yards Sum
              val rdLeftTackle = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
                broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "LEFT TACKLE")
              val sumLeftTackle = rdLeftTackle.agg(functions.sum("yards")).first.get(0)

              //Right Tackle Yards Sum
              val rdRightTackle = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
                broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "RIGHT TACKLE")
              val sumRightTackle = rdRightTackle.agg(functions.sum("yards")).first.get(0)

              //Center Yards Sum
              val rdCenter = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
                broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "CENTER")
              val sumCenter = rdCenter.agg(functions.sum("yards")).first.get(0)

              printf("%-15s%-15s%-15s%-15s%-15s%-15s%-15s%-15s\n","Name","Left End","Left Tackle","Left Guard","Center","Right Guard","Right Tackle","Right End")

              printf("%-15s%-15s%-15s%-15s%-15s%-15s%-15s%-15s\n","SF 49ers",sumLeftEnd,sumLeftTackle,sumLeftGuard,sumCenter,sumRightGuard,sumRightTackle,sumRightEnd)

              println()

            case "5" =>
              //QUERY 5
            broadcastDataNoRepar.value.select("DefenseTeam","Formation","PenaltyYards").groupBy("DefenseTeam","Formation").sum("PenaltyYards").where("DefenseTeam is not null").orderBy("DefenseTeam").withColumnRenamed("sum(PenaltyYards)","Total_Penalty_Yards").show()

            case "6" =>
              //QUERY 6
            broadcastDataNoRepar.value.select("OffenseTeam","yards","Formation").groupBy("OffenseTeam","Formation").sum("yards").where("Formation like 'SHOTGUN'").withColumnRenamed("sum(yards)","Yards in Shotgun Formation").orderBy(desc("Yards in Shotgun Formation")).show()


            case "7" =>
              //ADD A USER
              print("What is the name of the new user?: ")
              var newuser = StdIn.readLine()
              print("What is the password for this user?: ")
              var newpass = StdIn.readLine()
              var newencryptedpass = md5(newpass)

              var newpriv = ""

              breakable {
                do {
                  println("Would like to give this user admin or basic privileges?: ")
                  println("1) admin")
                  println("2) basic")
                  print("> ")
                  newpriv = StdIn.readLine()

                  newpriv match {
                    case "1" =>
                      newpriv = "admin"
                      break;
                    case "2" =>
                      newpriv = "basic"
                      break;
                    case _ =>
                      println(Console.RED + "ERROR, UNEXPECTED COMMAND: select a valid command from the selection menu" + Console.RESET)
                      println()
                  }

                } while (true)

              }

              statement.executeUpdate("INSERT INTO users (user_name,user_password,user_privileges) \n" +
                "VALUES ('" + newuser + "','" + newencryptedpass + "','" + newpriv + "');")

              println(Console.BLUE + "SUCCESS! USER HAS BEEN ADDED!" + Console.RESET)
              println()

            case "8" =>
              //DELETE A USER
              print("What is the User ID of the user you are trying to delete? ")
              var userid = StdIn.readLine()
              println()

              var sql = "SELECT * FROM users WHERE user_id = " + userid + ";"
              var resultSet = statement.executeQuery(sql)

              if(resultSet.next() == false){
                println(Console.YELLOW + "THIS USER DOES NOT EXISTS" + Console.RESET)
                println()
              }
              else{
                statement.executeUpdate("DELETE FROM users WHERE user_id = " + userid + ";")
                println(Console.BLUE + "USER DELETED SUCCESSFULLY" + Console.RESET)
                println()
              }

            case "9" => optionmenucheck = true
            case "thisdoesnothing" =>
            case _ => println(Console.RED + "ERROR, UNEXPECTED COMMAND: select a valid command from the selection menu" + Console.RESET)
              println()
          }

        } while (!optionmenucheck)


      }

    } while(!programexitcheck)

    println("Thank you for using OUR app, Goodbye!")

  }


  def md5(s: String):String = {
    var news=MessageDigest.getInstance("MD5").digest(s.getBytes)
    var news2 = news.mkString("")
    return news2
  }



}

