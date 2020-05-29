import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object tsyJoin {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("test").setMaster("local[*]")
    val sc=new SparkContext(conf)

    val input_rdd_1: RDD[String] = sc.textFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\result.txt")

    val uin_pos=1
    val time_pos=2
    val scene_pos=3
    val data_pos=4

    val like_scene="tlog_detail|wegame_detail|key_press"
    val validity=0
    val broad_validity=sc.broadcast(validity)
    val like_scene_list: Broadcast[String] = sc.broadcast(like_scene)

    //1534280186_3_3_|10000|wegame_detail|2020042222###3###1###3###11###192###
    //      * 1534280186_3_3_|10000|tlog_detail|2020042212###3###746###62745


    //    }
    val group_rdd: RDD[(String, Iterable[Array[String]])] = input_rdd_1.map(line => {
      val line_info: Array[String] = line.split('|')
      line_info(0) -> line_info
    }).groupByKey()
    val resut_rdd: RDD[ListBuffer[String]] = group_rdd.map({
      case (key, it) => {
        val out_res_list: ListBuffer[String] = ListBuffer[String]()
        var out_res_map:Map[String, String] = Map[String,String]()
        var out_res_key=""
        //          val list: List[Array[String]] = it.toList
        /**
          * 1534280186_3_3_|10000,t_log_detail|2020042222###3#,wegame_detail|2020042222###3###1###3###11###192###
          *
          */
        var finl_res_map: Map[String, Array[String]] = Map[String, Array[String]]()
        val like_scene = like_scene_list.value
        val scene_list: Array[String] = like_scene.split('|')
        val validity = broad_validity.value
        var main_scene = ""
        //如果存在主场景则主场景为分割后的第一个tlog_detail
        if (like_scene.length > 0) main_scene = like_scene.split('|')(0)

        val sorted_list: Array[Array[String]] = it.toArray.sortWith((arr1, arr2) => {
          arr1(2) < arr2(2) //按照data中的时间字段降序排列
        })

        var photo_time = 0
        //控制快照时间
        var cur_scene_time = 0
        //控制当前场景时间
        var photo_flag = false //控制是否要快照

        //构造一个场景map
        var AAA_status_map: Map[String, Array[String]] = Map[String, Array[String]]()
        //给场景填充默认值
        scene_list.foreach(scene => AAA_status_map += scene -> Array())

        /**
          * 1534280186_3_3_|10000|wegame_detail|2020042222###3###1###3###11###192###,
          * 1534280186_3_3_|10000|t_log_detail|2020042222###3#
          */

        sorted_list.foreach(arr => {
          val cur_scene_name = arr(2)
          cur_scene_time = arr(1).toInt //当前场景时间
          out_res_key=cur_scene_name+cur_scene_time

          //如果当前场景=主场景更新控制快照时间和快照标记
          /**
            * 1534280186_3_3_|10000,1534280186_3_3_|10000|wegame_detail|2020042222###3###1#
            * 1534280186_3_3_|10000,1534280186_3_3_|10000|wegame_detail|2020042222###3###1#
            * 1534280186_3_3_|10000,1534280186_3_3_|10000|wegame_detail|2020042222###3###1#
            */

          //如果要快照并且快照时间不为0并且当前场景时间大于控制快照时间
          if (photo_flag && photo_time != 0 && cur_scene_time > photo_time) {
            val final_res_list: ListBuffer[Array[String]] = ListBuffer[Array[String]]()
            var final_str = ""
            scene_list.foreach(scene => {
              val time_diff = photo_time - cur_scene_time
              val data: String = AAA_status_map.getOrElse(scene, Array())(3)
              if (time_diff <= validity) {
                final_str += data
                //                    final_res_list+=data.split('#')
              } else {
                final_str += "\\N\\N\\N\\N"
                //                  final_res_list+=Array("-1","-1")
              }
              !photo_flag
              out_res_list += final_str
            })
          }
          //主场景要到，要比主场景的时间大

          //向map里面填充数据
          AAA_status_map += cur_scene_name -> arr
          if (cur_scene_name == main_scene) {
            photo_time = cur_scene_time
            photo_flag = true
          }
        })


        //收尾
        if (photo_time == cur_scene_time) {
          val final_res_list: ListBuffer[Array[String]] = ListBuffer[Array[String]]()
          var final_str = ""
          scene_list.foreach(scene => {
            val time_diff = photo_time - cur_scene_time
            val data: String = AAA_status_map.getOrElse(scene, Array())(3)
            if (time_diff <= validity) {
              final_str += data
              //                    final_res_list+=data.split('#')
            } else {
              final_str += "\\N\\N\\N\\N"
              //                  final_res_list+=Array("-1","-1")
            }
            !photo_flag

            out_res_list += final_str
          })
        }
        out_res_list
      }
    })
    resut_rdd





      .foreach(println)





































    //    println(str)




    //     data_load(input_rdd_1,"5|2|14","0","tlog_detail")
    //          .foreach(println)
    //      input_rdd_1.map(line=>{
    //        val line_info: Array[String] = line.split("|")
    //
    //      })


    //    def data_load(input_rdd_1: RDD[String],uin_pos:String,time_pos:String,scene_name:String): RDD[String] ={
    //      input_rdd_1.map(line=>{
    //        val line_info: Array[String] = line.split('|')
    //        val uin_pos_list: Array[String] = uin_pos.split('|')
    //        var main_key=""
    //        uin_pos_list.foreach(str=>{
    //             main_key+=line_info(str.toInt-1)+"_"
    //        })
    //        main_key=main_key.substring(0,main_key.length-2)
    //        var second_key=""
    //        if (time_pos=="0")second_key="10000"
    //        main_key+"|"+second_key+"|"+scene_name+"|"+"###"+line
    //      })









  }

}
