import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

object test {

  def main(args: Array[String]): Unit = {


        val conf=new SparkConf().setAppName("test").setMaster("local[*]")
        val sc=new SparkContext(conf)

//    val str="10003007|1588661877|mac_id|2020-05-05 14:57:57###10003007###2A628D21D3C24B59###2020-05-05 14:57:57###9999-12-31 23:59:59"
//    val data_info: Array[String] = str.split('|')
//    var qq=data_info(2-1)
//    println(qq)

    println(Array.iterate("-1", 4)(_ => "-1").mkString("|") + "|")
//         val input_rdd: RDD[(String, Array[Int])] = sc.parallelize(Seq("a"->Array(1,1,1),"b"->Array(2,2,2),"c"->Array(3,3,3)))
//            val input_rdd_1: RDD[String] = sc.textFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\测试数据\\test.txt")

//    input_rdd_1.foreach(println)
//        input_rdd.groupByKey()
//      .map({
//        case (key,it)=>{
//          val list: List[Array[Int]] = it.toList
//          list.foreach(arr=>{
//            arr(2)
//          })
//        }
//      })
//      .foreach(println)

//   val res: Array[Int] = Array.iterate(-1,10)(_=> -1)
//    println(res.mkString("|"))

//    2020-04-22 22:32:46
//def time_convert(input_time: String)={
//  val format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//  val timeStamp: Long = format.parse(input_time).getTime/1000
//  timeStamp
//}

//    println(time_convert("2020-05-06 18:32:46"))
//    println("253402271999".toDouble)

//    val str="2020042222|3|1|3|11|2020-04-22 22:32:46|200218378|350|100|8|0|8|1|0|0|9224192|0|2|255|255|1534280186|3|200218378"
//    println(str.replaceAll("\\|", "###"))

//    val str="2020042212###3###746"
//    println(str.replaceAll("\\###","|"))


//    var main_key="1534280186_3_3_"
//    main_key=main_key.substring(0,main_key.length-1)
//    println(main_key)

//    var map: Map[String, String] = Map[String,String]("t_log"->"1")
//    map +=("t_log"->"2")
//    map.foreach(println)
//    val final_res_list: ListBuffer[Array[String]] = ListBuffer[Array[String]]()
//    final_res_list+=Array("1","2")
//    final_res_list.map(x=>{
//      var str=""
//      for (elem <- x) {str+=elem+"|"}
//      str
//    }).foreach(println)



//    val conf=new SparkConf().setAppName("test").setMaster("local[*]")
//    val sc=new SparkContext(conf)
////    val rdd1=sc.textFile("F:\\project\\TablesJoin\\data\\report.txt")
////    val rdd2=sc.textFile("F:\\project\\TablesJoin\\data\\report.txt")
//    val rdd1=sc.parallelize(Seq((1, 1) ,(1, 2) ,(1, 3)),2)
//    val rdd2=sc.parallelize(Seq((1, 4),(2, 1) ,(2, 2)),2)
//    rdd1.cogroup(rdd2).foreach(println)
//    println("--------------")
//    rdd1.join(rdd2).foreach(println)



//    rdd1.map(line=>{
//      line.split("|")
//      val qq = line(4)
//      val worldid = line(2)
//      val battleid = line(6)
//      (qq + "|" + worldid + "|" + battleid, line)
//    }).groupByKey()

  }


//  val input1: String = AB_Toolkits.get_arg("input1", "", "输入路径1")
//  val input2: String = AB_Toolkits.get_arg("input2", "", "输入路径2")
//  val input3: String = AB_Toolkits.get_arg("input3", "", "输入路径3")
//  val input4: String = AB_Toolkits.get_arg("input4", "", "输入路径4")
//  val input5: String = AB_Toolkits.get_arg("input5", "", "输入路径5")
//  val input6: String = AB_Toolkits.get_arg("input6", "", "输入路径6")
//  val input_column1: String = AB_Toolkits.get_arg("input_column1", "", "输入数据1列格式")
//  val input_column2: String = AB_Toolkits.get_arg("input_column2", "", "输入数据2列格式")
//  val input_column3: String = AB_Toolkits.get_arg("input_column3", "", "输入数据3列格式")
//  val input_column4: String = AB_Toolkits.get_arg("input_column4", "", "输入数据4列格式")
//  val input_column5: String = AB_Toolkits.get_arg("input_column5", "", "输入数据5列格式")
//  val input_column6: String = AB_Toolkits.get_arg("input_column6", "", "输入数据6列格式")
//
//  val output1: String = AB_Toolkits.get_arg("output1", "", "输出路径1")
//
//  val model_output: String = AB_Toolkits.get_arg("model_output", "", "模型输出路径")
//  val result_output: String = AB_Toolkits.get_arg("result_output", "", "模型报告输出路径")
//
//  // 其他系统参数请参考：xxxx
//
//  // 读取文件到RDD
//  val input_rdd_1 = AB_Toolkits.get_rdd_data(input1)
//  val input_rdd_2 = AB_Toolkits.get_rdd_data(input2)
//  val input_rdd_3 = AB_Toolkits.get_rdd_data(input3)
//  val input_rdd_4 = AB_Toolkits.get_rdd_data(input4)
//  val input_rdd_5 = AB_Toolkits.get_rdd_data(input5)
//  val input_rdd_6 = AB_Toolkits.get_rdd_data(input6)
//
//
//
//  // 统计词出现的次数
//  val counts = input_rdd_1.flatMap(s => s).map(w => (w, 1)).reduceByKey(_ + _)
//  input_rdd_1.groupBy(str=>{
//    str
//  })
//
//  // 转换到Dataframe（并注册成临时表进行sql分析）
//  val tlog_deail_df = AB_Toolkits.conv_rdd_to_df(input_rdd_1, input_column1, "tlog_detail_table")
//  val wegame_detail_df = AB_Toolkits.conv_rdd_to_df(input_rdd_2, input_column2, "wegame_datail_table")
//  val keypress_df = AB_Toolkits.conv_rdd_to_df(input_rdd_3, input_column3, "keypress_table")
//  val game_death_df = AB_Toolkits.conv_rdd_to_df(input_rdd_4, input_column4, "game_death_table")
//  val report_df = AB_Toolkits.conv_rdd_to_df(input_rdd_5, input_column5, "report_table")
//  val game_score_df = AB_Toolkits.conv_rdd_to_df(input_rdd_6, input_column6, "game_score_table")
//
//  input_df_1.show()
//  val output_df = spark.sql("select * from report_df")
//  output_df.show()
//
//  // 保存数据
//  AB_Toolkits.save_output(counts.map(row => row._1+"|"+row._2), output1)
}
