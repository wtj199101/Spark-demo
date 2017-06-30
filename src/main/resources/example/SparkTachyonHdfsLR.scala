package example

import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.spark.scheduler.InputFormatInfo
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.exp

/**
 * Logistic regression based classification.
 * This cn.spark.example uses Tachyon to persist rdds during computation.
 *
 * This is an cn.spark.example implementation for learning how to use Spark. For more conventional use,
 * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
 * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS based on your needs.
 */
object SparkTachyonHdfsLR {
  val D = 10   // Numer of dimensions
  val rand = new Random(42)

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an cn.spark.example!
        |Please use either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
        |org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
        |for more conventional use.
      """.stripMargin)
  }

  case class DataPoint(x: Vector[Double], y: Double)

  def parsePoint(line: String): DataPoint = {
    val tok = new java.util.StringTokenizer(line, " ")
    var y = tok.nextToken.toDouble
    var x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok.nextToken.toDouble; i += 1
    }
    DataPoint(new DenseVector(x), y)
  }

  def main(args: Array[String]) {

    showWarning()

    val inputPath = args(0)
    val sparkConf = new SparkConf().setAppName("SparkTachyonHdfsLR")
    val conf = new Configuration()
    val sc = new SparkContext(sparkConf,
      InputFormatInfo.computePreferredLocations(
        Seq(new InputFormatInfo(conf, classOf[org.apache.hadoop.mapred.TextInputFormat], inputPath))
      ))
    val lines = sc.textFile(inputPath)
    val points = lines.map(parsePoint _).persist(StorageLevel.OFF_HEAP)
    val ITERATIONS = args(1).toInt

    // Initialize w to a random value
    var w = DenseVector.fill(D){2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient = points.map { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)
    sc.stop()
  }
}
