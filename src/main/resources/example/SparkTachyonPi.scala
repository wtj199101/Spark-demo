package example

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.random

/**
 *  Computes an approximation to pi
 *  This cn.spark.example uses Tachyon to persist rdds during computation.
 */
object SparkTachyonPi {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkTachyonPi")
    val spark = new SparkContext(sparkConf)

    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices

    val rdd = spark.parallelize(1 to n, slices)
    rdd.persist(StorageLevel.OFF_HEAP)
    val count = rdd.map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)

    spark.stop()
  }
}
