package uds.pc

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.Date

import com.hadoop.compression.lzo.LzopCodec
import uds.pc.Const._
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}
import uds.pc.Daily_Business._
import org.apache.spark.SparkContext._
import uds.pc.TagRule_Configure.loadTagRule
import scala.collection.mutable.HashMap
import uds.pc.JsonUtil._


/**
 * Created by hdfs on 2015/7/23.
 * parse useraction
 */
object UserAction_Daily {

  def main (args: Array[String]) {

    if (args.length < 2) {
      System.err.println(
        "Usage: FlumeEventCount <target_dir> <checkpoint_dir>")
      System.exit(1)
    }

    val Array(usDir,outDir) = args
    val sparkConf = new SparkConf().setAppName("uds ua")
    sparkConf.set("spark.core.connection.ack.wait.timeout","120")
    sparkConf.set("spark.akka.timeout","180")

    val sc = new SparkContext(sparkConf)
    val job = new Job()
    job.getConfiguration().set("mapred.output.compress", "true")
    job.getConfiguration().set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec")
    val usLines = sc.textFile(usDir)
    FileUtil.deleteHdfsDiectory(outDir)

    //元组私有人群
    val tag_conf_file : Map[String ,String] = loadTagRule(sc.textFile(TAG_RULE_FILEPATH).toArray())
    val dict_array = sc.textFile(GROUP_DICT_FILEPATH).filter(_.nonEmpty).toArray()
    val group_dict = load_public_rule(dict_array)
    val ua_logs = usLines.filter(line => line.nonEmpty).map(line =>(getValue("uid",line),ua_parser(line,tag_conf_file,group_dict))).filter(_._2.nonEmpty).
     reduceByKey(_ + LEVEL_TWO + _).map(x => x._1 + Const.LEVEL_ONE + combine_value(x._2)).coalesce(50).saveAsTextFile(outDir,classOf[LzopCodec])

    sc.stop()
  }


  /*
  * combine rule_id + LEVEL_THR + sid + LEVEL_THR + 1 + LEVEL_TWO + rule_id + LEVEL_THR + sid + LEVEL_THR
  * example 10087womai110087prom.gome1 => 10087  2  [womai,prom.gome]
   */
  def combine_value (all_tags:String): String ={
    var ids = Set[String]()
    val map_tax = HashMap[String, Tax_Value]()
    all_tags.split(LEVEL_TWO).map(tag => {
      if (tag.nonEmpty && tag.split(LEVEL_THR).length>2){
        val rule_id = tag.split(LEVEL_THR)(0)
        val advs = tag.split(LEVEL_THR)(1)
        val weight = tag.split(LEVEL_THR)(2).toDouble.toInt
        ids += rule_id
        if (map_tax.contains(rule_id)){
          val haved_tax = map_tax.get(rule_id).get
          val current_weight = haved_tax.w + weight
          var current_advs = haved_tax.advs
          if (!haved_tax.advs.contains(advs)){
            current_advs = current_advs.::(advs)
          }
          map_tax += (rule_id -> Tax_Value(current_weight , rule_id ,current_advs))
        }else
          map_tax += (rule_id -> Tax_Value(weight , rule_id ,List(advs)))
      }
    })
    var tax_values = List[Tax_Value]()
    map_tax.foreach(e =>{
      val (k ,v) = e
      tax_values = tax_values.::(v)
    })

    val result_tags = ids.mkString(Const.LEVEL_THR) + LEVEL_ONE + toMyJson(tax_values)
    result_tags
  }

  def load_public_rule(group_dict : Array[String]) : HashMap[String,Set[String]]  = {
    val dict_map  = HashMap[String,Set[String]]()
    for(dict <- group_dict){
      if (dict.trim.nonEmpty){
        val records = dict.trim.split("\t")
        if(records.length >= 2){
          val key = URLDecoder.decode(records(0).replaceAll("%","%25"),"UTF-8")
          var value = Set[String]()
          for (i <- 1 to records.length-1){
            value += records(i)
          }
          if (dict_map.contains(key) && dict_map.get(key) != None){
            var key_dict: Set[String] = dict_map.get(key).get
            for(valu <-  value){
              key_dict += valu
            }
            dict_map += (key -> key_dict)
          }else{
            dict_map += (key -> value)
          }
        }
      }
    }
    dict_map
  }

}
