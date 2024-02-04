
import org.apache.spark.sql.SQLContext
// create a SQL Context
val sqlContext = new SQLContext(sc)
// import implicits package
import sqlContext.implicits._

//a. Create a Spark data frame from a CSV file which has the headers in the first row
val df = spark.read.option("header","true").csv("/home/user/Documents/Datasets/simple.csv")

//verify
df.show()

//b. Print the data frame’s schema.
df.printSchema()

//dataframe2.printSchema()
//.c. Convert the data frame to a RDD and display its contents. 
val convertDFtoRDD = df.rdd  //use the .rdd extension to convert to RDD.
convertDFtoRDD.collect().foreach(println)  //loop through each line and print the contents
//val distData = sc.parallelize(df) // distData is an RDD object

//d. Create a RDD by reading from a text file (create a text file or use $SPARK_HOME/README.md in
//the bigdata vm).

val RDDdataframe = sc.textFile("/home/user/spark-2.1.0-bin-hadoop2.7/README.md")

//e. Calculate the total length in characters, including white spaces, for all the lines in the
//$SPARK_HOME/README.md file.
RDDdataframe.map(_.length).sum()

//f. Count and display all the words as (String, Int) pairs, which occur in $SPARK_HOME/README.md file of the bigdata vm.
val wordCounts = RDDdataframe.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
wordCounts.collect()

//G.Write a program which does word count of the $SPARK_HOME/README.md file using Spark.
//Explain the reduction operation.
val counts = RDDdataframe.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
counts.collect()

//H. Factorial Program:
val l = List(0,1,2,3,4,5) //List array of numbers 0-5

//Define Factorial function.
def fact(n:Int):Int = {
  if (n == 0) 1
  else n*fact(n-1)
}

// Apply Factorial function to array and store factorials in new list
val l_fact = l.map( x => fact(x) )
//val l_fact2 = data.map( x => fact(x) )
//function to sum the values of the array/list.
def sum(xs: List[Int]): Int = {
  xs match {
    case x :: tail => x + sum(tail) // if there is an element, add it to the sum of the tail
    case Nil => 0 // if there are no elements, then the sum is 0
  }
}

//calculate total - sum the values of the factorial list.
val total = sum(l_fact)

//print the value.
println(total)


System.exit(0)