import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.Encoders

object Main extends App {
  val spark = SparkSession.builder().appName("FlightProject").config("spark.master", "local").getOrCreate()

  val flightSchema = new StructType()
    .add("destCountry", "string")
    .add("originCountry", "string")
    .add("count", "integer")

  var flightDataDF = spark.read.format("csv")
    .option("sep", ",")
    .option("header", "true")
    .schema(flightSchema)
    .load("C:\\Users\\gokul\\Downloads\\2015-summary.csv")

  flightDataDF = flightDataDF.sort(desc("count"))

  printf("Data sorted by travel count - 10 records")
  println(flightDataDF.show(10))

  final case class FlightDSCase(destCountry: String,
                            originCountry: String,
                            count: Int)
  val encoder = Encoders.product[FlightDSCase]

  val flightDS = flightDataDF.as[FlightDSCase](encoder)
  println(flightDS.show(3))

  flightDS.createOrReplaceTempView("flights")

  spark.sql("SELECT * FROM FLIGHTS LIMIT 10").show(10)

}
