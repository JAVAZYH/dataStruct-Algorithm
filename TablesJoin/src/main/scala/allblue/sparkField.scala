package allblue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType}
import org.apache.spark.sql._
import org.apache.spark.sql.types._



object sparkField {
  def main(args: Array[String]): Unit = {
//    val conf=new SparkConf().setAppName("test").setMaster("local[*]")
    val spark=SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    val sc=spark.sparkContext

//    val arr = Array(1,2,3,4,5)
//    println(arr.slice(0,arr.length-1).mkString("|"))

//    val spark=SparkSession.builder().appName("test").getOrCreate()
    val input_rdd: RDD[Array[String]] = sc.textFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\test1.txt").map(_.split('|'))





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
//        type_list += tmp_list(x)
                type_list += "STRING"
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
    val input_column="qq,BIGINT,name,BIGINT,sex,BIGINT,str,STRING"
    val input_df_1 = conv_rdd_to_df(input_rdd, input_column, "demo_table_1")(spark)


    input_df_1.rdd.map(arr=>arr.toSeq.slice(0,arr.length-1).mkString("|"))
      .foreach(println)


    //
//    val tmp_list = input_colu
    //    mn.split(",")
//    var input_column1=""
//
//    val column_list = ArrayBuffer[String]()
//    val type_list = ArrayBuffer[String]()
//    for (x <- 0 until (tmp_list.length, 2)){
//      column_list += tmp_list(x)
//    }
//    for (x <- 1 until (tmp_list.length, 2)){
//      type_list += "STRING"
//    }
//
//    for(x <- 0 to column_list.length-1){
//      input_column1+= column_list(x)+","+type_list(x)+","
//    }
//
//    input_column1=input_column1.substring(0,input_column1.length-1)
//    println(input_column1)
//    column_list.map(x=>{
//      x+","+type_list
//    })
//    conv_rdd_to_df(input_rdd,input_column)(spark)
//
//    val tmp_list = input_column.split(",")
//
//    val column_list = ArrayBuffer[String]()
//    val type_list = ArrayBuffer[String]()
//    for (x <- 0 until (tmp_list.length, 2)){
//      column_list += tmp_list(x)
//    }
//    for (x <- 1 until (tmp_list.length, 2)){
//      type_list += "String"
//    }
//    column_list.foreach(println)
//    type_list.foreach(println)



//    def get_rdd_data(data_path:String, separate:String = "\\|")(implicit sc: SparkContext) = {
//      val input_rdd = sc.textFile(data_path)
//
//      val data = input_rdd.map(x=>{
//        x.split(separate)
//      })
//      data
//    }


//    // RDD转换成Dataframe

  }

}
