package example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Usage: MultiBroadcastTest [slices] [numElem]
  */
object MultiBroadcastTest {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Multi-Broadcast Test")
    val sc = new SparkContext(sparkConf)

    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = new Array[Int](num)
    for (i <- 0 until arr1.length) {
      arr1(i) = i
    }

    val arr2 = new Array[Int](num)
    for (i <- 0 until arr2.length) {
      arr2(i) = i
    }

    val barr1 = sc.broadcast(arr1)
    val barr2 = sc.broadcast(arr2)
    val observedSizes: RDD[(Int, Int)] = sc.parallelize(1 to 10, slices).map { _ =>
      (barr1.value.size, barr2.value.size)
    }
    // Collect the small RDD so we can print the observed sizes locally.
    observedSizes.collect().foreach(i => println(i))

    sc.stop()
  }
}
