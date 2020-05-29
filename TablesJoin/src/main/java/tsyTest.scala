import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object tsyTest {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("test").setMaster("local[*]")
    val sc=new SparkContext(conf)
//    val input_rdd_1: RDD[String] = sc.textFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\0505真实数据\\5min.txt")
//      .filter(str=>str.split('|').length>4)
//    val input_rdd_2: RDD[String] = sc.textFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\0505真实数据\\game_detail_all_mac_id.txt")
//      .filter(str=>str.split('|').length>4)
////    val result_rdd: RDD[String] = input_rdd_1.filter(str => {
////      val qq: String = str.split('|')(4)
////      qq == "871218674"||qq=="1053435337"
////    })
//    val kv_rdd1: RDD[(String, String)] = input_rdd_1.map(str=>str.split('|')(4)->str)
//    val kv_rdd2: RDD[(String, String)] = input_rdd_2.map(str=>str.split('|')(4)->str)
//    val result_rdd: RDD[(String, (String, String))] = kv_rdd1.join(kv_rdd2)
//    result_rdd.foreach(println)
    val MAINKEY_STR: String = sc.getConf.get("spark.my.LIKE_SCENE")
    val key1=MAINKEY_STR.split('|')
    val key2=MAINKEY_STR.split('|')
    val TIME_STR=sc.getConf.get("spark.my.LIKE_SCENE")
    val time1=TIME_STR.split('|')
    val time2=TIME_STR.split('|')
    val TIME_VALID=sc.getConf.get("spark.my.TIME_VALID")

  }
}
