package allblue

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

object sparksqlTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("day01")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    /**
      * 1.tlog 有 mac 没有 ip 有
      * 2.tlog 有 mac 没有 ip没有
      * 3.tlog 有 mac 有 ip 没有
      * 3.tlog 有 mac 有 ip 有
      */


    val input1: String = "C:\\Users\\aresyhzhang\\Desktop\\临时\\sparksql组件测试\\tlog_all.txt"
    val input2: String = "C:\\Users\\aresyhzhang\\Desktop\\临时\\sparksql组件测试\\mac_id.txt"
    val input3: String = "C:\\Users\\aresyhzhang\\Desktop\\临时\\sparksql组件测试\\ip.txt"
//    val input3: String = "C:\\Users\\aresyhzhang\\Desktop\\临时\\sparksql组件测试\\ip.txt"





    val input_column1: String = "tlog_detail_tdbank_imp_date,STRING,tlog_detail_worldid,BIGINT,tlog_detail___tablename,STRING,tlog_detail_ieventid,BIGINT,tlog_detail_iuin,STRING,tlog_detail_dteventtime,STRING,tlog_detail_dtgamestarttime,STRING"
    val input_column2: String = "mac_log_time,STRING,mac_account_id,STRING,mac_hexmopenid,STRING,mac_cur_dw_start_date,STRING,mac_cur_dw_end_date,STRING"
//    val input_column3: String = "tmp,BIGINT"
    val input_column3: String = "qq,BIGINT,ip_time,STRING,ip,STRING"
//    val input_column4: String = ""
//    val input_column5: String = ""


    // 其他系统参数请参考：xxxx

    // 读取文件到RDD
    def get_rdd_data(data_path:String, separate:String = "\\|")(implicit sc: SparkContext) = {
      val input_rdd = sc.textFile(data_path)

      val data = input_rdd.map(x=>{
        x.split(separate)
      })
      data
    }
    val input_rdd1= get_rdd_data(input1)(sc)
    val input_rdd2 = get_rdd_data(input2)(sc)
    val input_rdd3 = get_rdd_data(input3)(sc)


    def reg_df_table(df:DataFrame,table_name:String) :String={
      df.createOrReplaceTempView(table_name)
      table_name
    }

    //将输入格式全部映射为STRING
    def column_to_string(input:String) :String={
      //将输入格式全部映射为STRING
      val tmp_list = input.split(",")
      var input_column_tmp=""

      val column_list = ArrayBuffer[String]()
      val type_list = ArrayBuffer[String]()
      for (x <- 0 until (tmp_list.length, 2)){
        column_list += tmp_list(x)
      }
      for (x <- 1 until (tmp_list.length, 2)){
        type_list += "STRING"
      }

      for(x <- 0 to column_list.length-1){
        input_column_tmp+= column_list(x)+","+type_list(x)+","
      }

      input_column_tmp.substring(0,input_column_tmp.length-1)
    }
    // RDD转换成Dataframe
    def conv_rdd_to_df(data:RDD[Array[String]], input_column:String, table_name:String = "")(implicit spark: SparkSession) = {
      // data:rdd数据，已经通过分割符分割
      // input_column:输入列格式，str类型，列名,类型,类名,类型...,例如：open_id,DOUBLE,log_time,STRING
      // table_name:dataframe表名，table_name=""时，不会注册临时表
      val tmp_list = input_column.split(",")

      val column_list = ArrayBuffer[String]()
      val type_list = ArrayBuffer[String]()
      for (x <- 0 until (tmp_list.length, 2)){
        column_list += tmp_list(x)
      }
      for (x <- 1 until (tmp_list.length, 2)){
        type_list += tmp_list(x)
      }

      val calc_column_info = column_list.map(x=>{
        (column_list.indexOf(x), x, type_list(column_list.indexOf(x)))
      })

      val schema = StructType(calc_column_info.map(info=>{
        if(info._3 == "STRING"){
          StructField(info._2, StringType, true)
        }else{
          StructField(info._2, DoubleType, true)
        }
      }))

      val calc_rdd = data.map(line=>{
        val t = calc_column_info.map(info=>{
          try{
            if(info._3 == "STRING"){
              line(info._1).toString
            }else{
              line(info._1).toDouble
            }
          }
          catch{
            case e:Exception => {
              null
            }
          }
        })
        Row.fromSeq(t.toSeq)
      })

      val df = spark.createDataFrame(calc_rdd, schema)

      if (table_name != ""){
        df.createOrReplaceTempView(table_name)
      }
      df
    }


    val column1_string=column_to_string(input_column1)
    val column2_string=column_to_string(input_column2)
    val column3_string=column_to_string(input_column3)


    // 转换到Dataframe（并注册成临时表进行sql分析）
    val input_df_1 = conv_rdd_to_df(input_rdd1, column1_string, "input_table_1")(spark)
    val input_df_2 = conv_rdd_to_df(input_rdd2, column2_string, "input_table_2")(spark)
    val input_df_3 = conv_rdd_to_df(input_rdd3, column3_string, "input_table_3")(spark)

//    def time2long(time:String) ={
    ////      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    ////      val timeStamp: Long = format.parse(time).getTime/1000
    ////      timeStamp
    ////    }

    //对输入的参数进行解析
//    val MAINKEY_STR ="tlog_detail_iuin|mac_account_id|qq"
    val MAINKEY_STR ="tlog_detail_iuin|mac_account_id|qq"
    val key1=MAINKEY_STR.split('|')(0)
    val key2=MAINKEY_STR.split('|')(1)
    var key3=""
    val TIME_STR="tlog_detail_dteventtime|mac_cur_dw_start_date|ip_time"
//    val TIME_STR="tlog_detail_dteventtime|mac_cur_dw_start_date|ip_time"
    val time1=(TIME_STR.split('|')(0))
    val time2=(TIME_STR.split('|')(1))
    var time3=""
    val TIME_VALID="86400"
    //判断是否进行三表关联
    if (MAINKEY_STR.split('|').length==3){
      key3=MAINKEY_STR.split('|')(2)
      time3=TIME_STR.split('|')(2)
    }


    println(key1+"____"+key2+"____"+time1+"____"+time2+"______"+TIME_VALID)

    val input_table_1="input_table_1"
    val input_table_2="input_table_2"
    val input_table_3="input_table_3"

    //执行sql语句
    def execeute_sql(table1:String,table2:String,key2:String,time2:String) ={
      println(table1+"========="+table2+"============"+key2+"============="+time2)
      //定义sql
      val sqlStr=
        s"""
           |with tmp as (
           |select * from ${table1} a
           |left join ${table2} b
           |on a.${key1} = b.${key2}
           |and (unix_timestamp(a.${time1})-unix_timestamp(b.${time2}))between 0 and ${TIME_VALID}
           |),tmp2 as (
           |select
           |*,
           |row_number() over(partition by ${key1},${time1} order by ${time2} desc) rk
           |from tmp
           |where tmp.${time2} is not null
           |),tmp3 as (
           |select * from tmp2
           |where  rk<=1
           |),tmp4 as (
           |select *,
           |0 rk
           |from tmp
           |where ${time2} is null
           |)
           |select * from tmp3
           |union
           |select * from tmp4
      """.stripMargin
      val output_df = spark.sql(sqlStr)
      //去除最后一列窗口rk字段
      output_df.drop("rk")
    }

    var out_df:DataFrame=null
    var res_df:DataFrame=null
    input_column3.split(',')(0) match {
      case "tmp"=>{
        out_df = execeute_sql(input_table_1,input_table_2,key2,time2)
        res_df=out_df
      }
      case _=>{
        out_df = execeute_sql(input_table_1,input_table_2,key2,time2)
        val res_tmp: String = reg_df_table(out_df,"res_tmp")
        res_df= execeute_sql(res_tmp,input_table_3,key3,time3)
      }
    }

    out_df.show(20)
    res_df.show(20)
    println("res===============")
    // 保存数据
    res_df.rdd.map(_.mkString("|"))
      .foreach(println)
  }

}
