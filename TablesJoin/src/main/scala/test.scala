import com.sun.deploy.nativesandbox.comm.Response
import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat
import java.util

import scala.collection.JavaConversions._
import collection.JavaConverters._
import scala.collection.mutable.Map
import Array._
import scala.collection.mutable

object test2 {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val string="a|b|c|d"
//   string.split("\\|", 0).foreach(println)

//    println(0 % 50)

//    for (i <- 1 to  10){
//      println(i)
//    }
//var pip_res_map_tmp : mutable.Map[String, Response[java.util.Map[String,String] ] ] =()


    val test_map= mutable.Map[String, Response[java.util.Map[String,String] ]]()
    Map<String,String> test = new util.HashMap<>()
    test_map.+=("1"->java.util.Map("1"->"4"))
    test_map.+=("2"->java.util.Map("2"->"4"))
    test_map.+=("3"->java.util.Map("3"->"4"))
    test_map.keys.foreach(i=>{
      val foreign_key_flag = i.split("#%#",0).head
      var item_dict = test_map(i).get()
      println(foreign_key_flag)
    })

//    val test="23_uin_lol_game_detail_1534280186#3"
//    println(test.split("#%#", 0).head)

  }
}
