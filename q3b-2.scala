

import org.apache.spark.sql.SQLContext
// create a SQL Context
val sqlContext = new SQLContext(sc)
// import implicits package
import sqlContext.implicits._
import org.apache.spark.sql.functions._


import breeze.util.BloomFilter

val nums = List(1 to 100: _*).map(_.toString)
val rdd = sc.parallelize(nums, 5)

val bf = rdd.mapPartitions { iter =>
  val bf = BloomFilter.optimallySized[String](10000, 0.001)
  iter.foreach(i => bf += i)
  Iterator(bf)
}.reduce(_ | _)

println("Does it contain 5?", bf.contains("5")) // true
println("Does it contain 121?", bf.contains("121")) // false

System.exit(0)