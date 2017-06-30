package example

/**
  * Created by Administrator on 2017/06/30.
  */
object ExceptionHandlingTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ExceptionHandlingTest")
    val sc = new SparkContext(sparkConf)
    sc.parallelize(0 until sc.defaultParallelism).foreach { i =>
      if (math.random > 0.75) {
        throw new Exception("Testing exception handling")
      }
    }

    sc.stop()
  }
}
