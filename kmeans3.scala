import org.apache.spark.SparkConf
// $example on$
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
// $example off$
case class Record(word: String)
// Force Spark to talk less and work more!
sc.setLogLevel("OFF")
sc.stop()
/**
 * Estimate clusters on one stream of data and make predictions
 * on another stream, where the data streams arrive as text files
 * into two different directories.
 *
 * The rows of the training text files must be vector data in the form
 * `[x1,x2,x3,...,xn]`
 * Where n is the number of dimensions.
 *
 * The rows of the test text files must be labeled data in the form
 * `(y,[x1,x2,x3,...,xn])`
 * Where y is some identifier. n must be the same for train and test.
 *
 * Usage:
 *   StreamingKMeansExample <trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>
 *
 * To run on your local machine using the two directories `trainingDir` and `testDir`,
 * with updates every 5 seconds, 2 dimensions per data point, and 3 clusters, call:
 *    $ bin/run-example mllib.StreamingKMeansExample trainingDir testDir 5 3 2
 *
 * As you add text files to `trainingDir` the clusters will continuously update.
 * Anytime you add text files to `testDir`, you'll see predicted labels using the current model.
 *
 */
//val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingKMeans")
// Set batch interval of 1 second.
//val ssc = new StreamingContext(conf, Seconds(1))

    // $example on$
    val conf = new SparkConf().setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))

  //  val trainingData = ssc.textFileStream(args(0)).map(Vectors.parse)
  //  val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)

val trainlines = ssc.socketTextStream("10.0.0.3", 2999)
val testlines = ssc.socketTextStream("10.0.0.3", 7777)

val trainingData = trainlines.map(Vectors.parse).cache()
val testData = testlines.map(LabeledPoint.parse)

    val model = new StreamingKMeans()
      .setK(args(3).toInt)
      .setDecayFactor(1.0)
      .setRandomCenters(args(4).toInt, 0.0)

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
    // $example off$

System.exit(0)


