import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PreTestData {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("test").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val input_rdd: RDD[String] = sc.textFile("F:\\TSYTestData\\input\\game_detail_all_mac_id.txt")

    input_rdd.filter(str=>{
      val data_info: Array[String] = str.split('|')
      var qq=data_info(2-1).toLong
      qq%10==
//      try {
//        val numqq: Long = qq.substring(qq.length-4).toLong
//        numqq<2000
//    }catch {
//        case e:Exception=>(e.printStackTrace)
//          false
//      }
      true
    })
      input_rdd
      .repartition(1)
//      .foreach(println)
      .saveAsTextFile("F:\\TSYTestData\\out\\mac_id.txt")
  }
}
