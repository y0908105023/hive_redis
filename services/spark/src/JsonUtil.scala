package uds.pc

import net.liftweb.json._
import net.liftweb.json.JsonDSL._

/**
 * Created by hdfs on 2015/7/29.
 */

case class Tax_Value(w : Int ,id : String ,advs : List[String])
object JsonUtil {


  def getValue(key : String,json : String): String ={
    val value = (parse(json) \ key \\ classOf[JString])
    if (value == JNothing || value.size == 0)
      ""
    else
      value(0)
  }

  def getIntValue(key : String,json : String): String ={
    val value = (parse(json) \ key \\ classOf[JInt])
    if (value == JNothing || value.size == 0)
      ""
    else
      value(0).toString()
  }

  def getValue(key1 : String , key2 : String , json : String): String ={
    val value = (parse(json) \ key1 \ key2 \\ classOf[JString])
    if (value == JNothing || value.size == 0)
      ""
    else
      value(0)
  }

  /*
  *   key : json key
  *   json : json String
  *   return JArray[Any]
  *   example : {"name":["mikcle","jack"]}
   */
  def getValues(key : String , json : String): JArray#Values ={
    (parse(json) \ key \\ classOf[JArray])(0)
  }

  def contains(key : String ,json : String): Boolean ={
    (parse(json) \ key \\ classOf[JString]).size != 0
  }

  def parser(json : String): JValue = {
    parse(json)
  }

  def toMyJson(tax_value : List[Tax_Value]): String ={
    val json = ("tax" -> tax_value.map{
      ww => (( "w" -> ww.w) ~
        ("id" -> ww.id) ~
        ("advs" -> ww.advs))
    })
    compact(render(json))
  }

  def main (args: Array[String]) {
//    val json = parse("{\"tax\": [{\"w\": \"1\", \"id\": \"100005\", \"advs\": [\"51wan\"]}, {\"w\": \"1\", \"id\": \"19\", \"advs\": [\"51wan\"]}]}")
//    json \ "tax"
//    compact(render(json))

    val tax1 = List(Tax_Value(1,"22",List("aa","bb")),Tax_Value(2,"33",List("cc","dd")))
    println(toMyJson(tax1))

    val aa = "{\"version\":\"1.0.0\",\"type\":1,\"uid\":\"640CDk6I7Uq7khgE\",\"isNew\":0,\"sid\":\"sfbest\",\"sessionid\":\"0\",\"serverTime\":1438102741,\"url\":\"http://www.sfbest.com/html/products/149/1300148877.html\",\"userAgent\":\"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)\",\"ip\":\"60.181.134.180\",\"others\":{\"tid\":\"1438102704042\",\"ms\":\"1438102704043\",\"ref\":\"http://www.sfbest.com/html/activity/1437707962.html\",\"title\":\"茂昽 7.28-8.3“万人团“活动进行中 活动结束恢复199元 8月初发货 香格里拉的新鲜松茸家庭装 500g 【品牌 价格 行情 评价 图片】 - 顺丰优选sfbest.com\",\"bfs\":\"ua=Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E)\\u0000tz=-8\\u0000res=1920x1040x1920x1080x24x1920x985x0x55\\u0000hal=zh-CN\\u0000ha=image/png, image/svg+xml, image/*;q=0.8, */*;q=0.5\\u0000hae=gzip,deflate\"}}"

  }

}
