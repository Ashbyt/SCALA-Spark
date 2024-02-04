

import org.apache.spark.SparkContext._
sc.setLogLevel("OFF")
import spark.implicits._
import org.apache.spark.sql.SQLContext
// create a SQL Context
val sqlContext = new SQLContext(sc)
// import implicits package
import sqlContext.implicits._


case class myFlights(UniqueCarrier: String, Origin: String) //, Dest: String, DepDelay: String, ArrDelay: String)

//val df = spark.read.option("header","true").parquet("/home/user/Documents/Datasets/flight_2008_pq.parquet").as[myFlights]
val df = spark.read.option("header","true").parquet("/Users/Ash/Assignment2/flight_2008_pq.parquet").as[myFlights]


//val df11 = df.filter((df.col("ArrDelay").isNotNull))
//val flightDelays2 = df11.groupBy(df.col("FlightNum")).agg(avg(df11.col("ArrDelay")), avg(df11.col("DepDelay")))

val flightDelays = df.groupBy(df.col("FlightNum")).agg(avg(df.col("ArrDelay")), avg(df.col("DepDelay")))
flightDelays.cache()


//val flightDelays3 = flightDelays.filter((flightDelays($"AVG(ArrDelay)").isNotNull))
//flightDelays.filter(df.col("AVG(ArrDelay)").isNotNull()).sort($"AVG(ArrDelay)".desc).show()
//flightDelays.filter(df.col("AVG(DepDelay)").isNotNull()).sort($"AVG(DepDelay)".desc).show()

//flightDelays.filter((df.col("AVG(ArrDelay)").isNotNull()).sort($"AVG(ArrDelay)").show()
//flightDelays.filter(df.col("AVG(DepDelay)").isNotNull()).sort($"AVG(DepDelay)").show()


//Descending.
println("Descending")
flightDelays.sort($"AVG(ArrDelay)".desc).show()
flightDelays.sort($"AVG(DepDelay)".desc).show()

//Ascending.
println("Ascending")
flightDelays.sort($"AVG(ArrDelay)").show()
flightDelays.sort($"AVG(DepDelay)").show()

System.exit(0)
