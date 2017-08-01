package uds.pc

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import uds.pc.JsonUtil._
import scala.collection.mutable.HashMap
import com.hadoop.compression.lzo.LzopCodec

/**
 * Created by hdfs on 2015/7/27.
 */
object UserAction_Whole {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumeEventCount <target_dir> <checkpoint_dir>")
      System.exit(1)
    }
    val Array(inDir,outDir) = args
    val sparkConf = new SparkConf().setAppName("uds ua merge")
    sparkConf.set("spark.core.connection.ack.wait.timeout","120")
    sparkConf.set("spark.akka.timeout","180")

    val sc = new SparkContext(sparkConf)
    val job = new Job()
    job.getConfiguration().set("mapred.output.compress", "true")
    job.getConfiguration().set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec")
    val usLines = sc.textFile(inDir)
    FileUtil.deleteHdfsDiectory(outDir)

    usLines.map(line => (line.split(Const.LEVEL_ONE)(0),List().::(line.split(Const.LEVEL_ONE)(2)).::(line.split(Const.LEVEL_ONE)(1)).mkString(Const.LEVEL_ONE))).
      reduceByKey(combine_value(_,_)).map(x => x._1 + Const.LEVEL_ONE + x._2).coalesce(60).saveAsTextFile(outDir ,classOf[LzopCodec])

    sc.stop()
  }

  /*
  * combine the values
  * example  tag1_from_user = 100022{"tax":[{"w":2,"id":"100022","advs":["prom.gome"]}]}
  *          tag2_from_user = 10002230{"tax":[{"w":1,"id":"100022","advs":["prom.gome"]},{"w":1,"id":"30","advs":["prom.gome"]}]}
  *          ===============> 10002210002230{"tax":[{"w":3,"id":"100022","advs":["prom.gome"]},{"w":1,"id":"30","advs":["prom.gome"]}]}
   */
  def combine_value(tag1_from_user : String,tag2_from_user : String): String ={

    val tag1_from_user_list = tag1_from_user.split(Const.LEVEL_ONE)
    val tag2_from_user_list = tag2_from_user.split(Const.LEVEL_ONE)

    var users_rule_id = Set[String]()

    tag1_from_user_list(0).split(Const.LEVEL_THR).foreach(id1 => tag2_from_user_list(0).split(Const.LEVEL_THR).foreach(id2 => {
      users_rule_id += id1
      users_rule_id += id2
    }))

    val taxJson1_from_user = getValues("tax" , tag1_from_user_list(1))
    val taxJson2_from_user = getValues("tax" , tag2_from_user_list(1))

    var tax_in_dict = HashMap[String , Tax_Value]()
    for (one_tax_json <- taxJson1_from_user){
      val one_tax_map = one_tax_json.asInstanceOf[Map[String,Any]]
      val rule_id = one_tax_map.get("id").get.toString
      val weight = one_tax_map.get("w").get.toString.toDouble.toInt
      val advs = one_tax_map.get("advs").get.asInstanceOf[List[String]]
      tax_in_dict += (rule_id -> Tax_Value(weight , rule_id ,advs))
    }

    for (one_tax_json <- taxJson2_from_user){
      var rule_map = one_tax_json.asInstanceOf[Map[String,Any]]
      val rule_id = rule_map.get("id").get.toString
      val weight = rule_map.get("w").get.toString.toDouble.toInt
      val advs = rule_map.get("advs").get.asInstanceOf[List[String]]

      if (tax_in_dict.contains(rule_id)){
        val haved_tax = tax_in_dict.get(rule_id).get

        val current_weight = haved_tax.w + weight
        val haved_advs = haved_tax.advs
        var current_advs = haved_tax.advs
        advs.foreach(adv => {
          if (!haved_tax.advs.contains(adv)){
            current_advs = current_advs.::(adv)
          }
        })
        tax_in_dict += (rule_id -> Tax_Value(current_weight , rule_id , current_advs))
      }else
        tax_in_dict += (rule_id -> Tax_Value(weight , rule_id ,advs))
    }

    var tax_values = List[Tax_Value]()
    tax_in_dict.foreach(e =>{
      val (k ,v) = e
      tax_values = tax_values.::(v)
    })

    users_rule_id.mkString(Const.LEVEL_THR) + Const.LEVEL_ONE + toMyJson(tax_values)
  }

}
