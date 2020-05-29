import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BetterJoin {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("test").setMaster("local[*]")
    val sc=new SparkContext(conf)

////    val input_rdd: RDD[String] = sc.textFile("")
//    val input_rdd= sc.parallelize(Seq(1,2,2,2,2,2,3,4,5,5,5,5,5,5,5))
//      .map(_->1)
//      .sample(false,0.5)
//      .groupByKey()
//      .repartition(200)
//      .foreach(println)
    val MAINKEY_STR: String = "tlog_detail_iuin|mac_account_id"

    def time2long(time:String) ={
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val timeStamp: Long = format.parse(time).getTime/1000
      timeStamp
    }
//    //对输入的参数进行解析
//    val MAINKEY_STR = sc.getConf.get("spark.my.MAINKEY_STR")
//    val key1=MAINKEY_STR.split('|')(0)
//    val key2=MAINKEY_STR.split('|')(1)
//    val TIME_STR=sc.getConf.get("spark.my.TIME_STR")
//    val time1=time2long(TIME_STR.split('|')(0))
//    val time2=time2long(TIME_STR.split('|')(1))
//    val time3=time2long(TIME_STR.split('|')(1))
//    val TIME_VALID=sc.getConf.get("spark.my.TIME_VALID")


//    val time_diff_sub=(time2-time1)+(time3-time1)

//    定义执行sql
//    val sqlStr=
//      s"""
//         |select *
//         |from
//         |(
//         |select *,
//         |row_number() over(partition by ${key1},${time1} order by time_diff desc) rk
//         |from
//         |(select
//         |*,
//         |${time_diff_sub} time_diff
//         |from
//         |(
//         |select * from input_table_1 a
//         |left join input_table_2 b
//         |on a.${key1} = b.${key2}
//         |where (unix_timestamp(a.${time1})- unix_timestamp(b.${time2}))between 0 and ${TIME_VALID}
//         |) c
//         |) d
//         |) e
//         |where rk<=1
//      """.stripMargin
//
//    val sql2=
//      s"""
//        |select * from(
//        |
//        |)
//      """.stripMargin+sqlStr

  }
}
