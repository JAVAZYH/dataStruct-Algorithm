package allblue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object sparkRDD {
    def main(args: Array[String]): Unit = {
      val conf=new SparkConf().setAppName("test").setMaster("local[2]")
      val sc=new SparkContext(conf)

      val input1: String = "C:\\Users\\aresyhzhang\\Desktop\\临时\\sparksql组件测试\\tlog_all.txt"
      val input2: String = "C:\\Users\\aresyhzhang\\Desktop\\临时\\sparksql组件测试\\mac_id.txt"
      val input3: String = "C:\\Users\\aresyhzhang\\Desktop\\临时\\sparksql组件测试\\ip.txt"

      val input_rdd1: RDD[String] = sc.textFile(input1)
      val input_rdd2: RDD[String] = sc.textFile(input2)
      val input_rdd3: RDD[String] = sc.textFile(input3)

      val two_join_rdd: RDD[(String, (String, Option[String]))] = input_rdd1.map(str => {
        val key: String = str.split('|')(5 - 1)
        key -> str
      }).leftOuterJoin(input_rdd2.map(str => {
        val key: String = str.split('|')(2 - 1)
        key -> str
      }))
      two_join_rdd
          .leftOuterJoin( input_rdd3.map(str=>{
            val key: String = str.split('|')(1-1)
            key->str
          }))
          .groupByKey()
          .map(it=>{
            it.toString()
//            it.map(str,it2=>{
//              it2.toString
//            })
            null
          })
        .foreach(println)



//      input_rdd2.map(str=>{
//        val key: String = key.split('|')(5-1)
//        key->str
//      })
//      input_rdd3.map(str=>{
//        val key: String = key.split('|')(5-1)
//        key->str
//      })





    }


}
