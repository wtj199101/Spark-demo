package example

import java.nio.ByteBuffer
import java.util.{Arrays, SortedMap}

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/06/30.
  */
/*
 * This cn.spark.example demonstrates using Spark with Cassandra with the New Hadoop API and Cassandra
 * support for Hadoop.
 *
 * To run this cn.spark.example, run this file with the following command params -
 * <cassandra_node> <cassandra_port>
 *
 * So if you want to run this on localhost this will be,
 * localhost 9160
 *
 * The cn.spark.example makes some assumptions:
 * 1. You have already created a keyspace called casDemo and it has a column family named Words
 * 2. There are column family has a column named "para" which has test content.
 *
 * You can create the content by running the following script at the bottom of this file with
 * cassandra-cli.
 *
 */
object CassandraTest {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("casDemo")
    // Get a SparkContext
    val sc = new SparkContext(sparkConf)

    // Build the job configuration with ConfigHelper provided by Cassandra
    val job = new Job()
    job.setInputFormatClass(classOf[ColumnFamilyInputFormat])

    val host: String = args(1)
    val port: String = args(2)

    ConfigHelper.setInputInitialAddress(job.getConfiguration(), host)
    ConfigHelper.setInputRpcPort(job.getConfiguration(), port)
    ConfigHelper.setOutputInitialAddress(job.getConfiguration(), host)
    ConfigHelper.setOutputRpcPort(job.getConfiguration(), port)
    ConfigHelper.setInputColumnFamily(job.getConfiguration(), "casDemo", "Words")
    ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "casDemo", "WordCount")

    val predicate = new SlicePredicate()
    val sliceRange = new SliceRange()
    sliceRange.setStart(Array.empty[Byte])
    sliceRange.setFinish(Array.empty[Byte])
    predicate.setSlice_range(sliceRange)
    ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate)

    ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner")
    ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner")

    // Make a new Hadoop RDD
    val casRdd = sc.newAPIHadoopRDD(
      job.getConfiguration(),
      classOf[ColumnFamilyInputFormat],
      classOf[ByteBuffer],
      classOf[SortedMap[ByteBuffer, IColumn]])

    // Let us first get all the paragraphs from the retrieved rows
    val paraRdd = casRdd.map {
      case (key, value) => {
        ByteBufferUtil.string(value.get(ByteBufferUtil.bytes("para")).value())
      }
    }

    // Lets get the word count in paras
    val counts = paraRdd.flatMap(p => p.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    counts.collect().foreach {
      case (word, count) => println(word + ":" + count)
    }

    counts.map {
      case (word, count) => {
        val colWord = new org.apache.cassandra.thrift.Column()
        colWord.setName(ByteBufferUtil.bytes("word"))
        colWord.setValue(ByteBufferUtil.bytes(word))
        colWord.setTimestamp(System.currentTimeMillis)

        val colCount = new org.apache.cassandra.thrift.Column()
        colCount.setName(ByteBufferUtil.bytes("wcount"))
        colCount.setValue(ByteBufferUtil.bytes(count.toLong))
        colCount.setTimestamp(System.currentTimeMillis)

        val outputkey = ByteBufferUtil.bytes(word + "-COUNT-" + System.currentTimeMillis)

        val mutations = Arrays.asList(new Mutation(), new Mutation())
        mutations.get(0).setColumn_or_supercolumn(new ColumnOrSuperColumn())
        mutations.get(0).column_or_supercolumn.setColumn(colWord)
        mutations.get(1).setColumn_or_supercolumn(new ColumnOrSuperColumn())
        mutations.get(1).column_or_supercolumn.setColumn(colCount)
        (outputkey, mutations)
      }
    }.saveAsNewAPIHadoopFile("casDemo", classOf[ByteBuffer], classOf[List[Mutation]],
      classOf[ColumnFamilyOutputFormat], job.getConfiguration)

    sc.stop()
  }
}
