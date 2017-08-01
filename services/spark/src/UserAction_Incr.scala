package uds.pc

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}
import com.hadoop.compression.lzo.LzopCodec
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashSet

/**
 * Created by hdfs on 2015/7/28.
 */
object UserAction_Incr {

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println(
        "Usage: FlumeEventCount <target_dir> <checkpoint_dir>")
      System.exit(1)
    }
    val Array(wholeDir,dailyDir,outDir) = args
    val sparkConf = new SparkConf().setAppName("uds ua merge")
    sparkConf.set("spark.core.connection.ack.wait.timeout","120")
    sparkConf.set("spark.akka.timeout","180")
    
    val sc = new SparkContext(sparkConf)
    val job = new Job()
    job.getConfiguration().set("mapred.output.compress", "true")
    job.getConfiguration().set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec")
    val wholeLines = sc.textFile(wholeDir)
    val daily = sc.textFile(dailyDir).map(line => (line.split(Const.LEVEL_ONE)(0) , line))
    FileUtil.deleteHdfsDiectory(outDir)
    wholeLines.map(line => (line.split(Const.LEVEL_ONE)(0) , line)).leftOuterJoin(daily).filter(x => x._2._2.nonEmpty).map(_._2._2.get).coalesce(40).saveAsTextFile(outDir ,classOf[LzopCodec])

    sc.stop()
  }
}
