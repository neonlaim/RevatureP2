import org.apache.spark.sql.{Encoder, Encoders}

object CustomImplicits {
  case class NFL(GameId: Int, GameDate: String, OffenseTeam: String, DefenseTeam: String, Description: String, SeasonYear: Int,
                 Yards: Int, Formation: String, IsRush: Int,IsPass: Int, IsSack: Int, IsPenalty: Int, RushDirection: String, PenaltyYards: Int)
  implicit val nflEncoder: Encoder[NFL] = Encoders.product[NFL]
}
