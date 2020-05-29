import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object tsyDoubleJoinJoin {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("test").setMaster("local[*]")
    val sc=new SparkContext(conf)

    val input_rdd_1: RDD[String] = sc.textFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\测试数据\\all.txt")


//    val like_scene="game_detail_all|mac_id"
//    val validity="3600".toLong
        val like_scene="tlog_detail58|wegame_detail23|keypress28"
        val validity="0".toLong


    val broad_validity=sc.broadcast(validity)
    val like_scene_list: Broadcast[String] = sc.broadcast(like_scene)
    val group_rdd: RDD[(String, Iterable[Array[String]])] = input_rdd_1.map(line => {
      val line_info: Array[String] = line.split('|')
      line_info(0) -> line_info
    }).groupByKey()

    input_rdd_1.map(line => {
      val line_info: Array[String] = line.split('|')
      line_info(0) -> line_info
    }).sample(false,0.1)
      .countByKey()
      .foreach(println)

    val result_rdd: RDD[ListBuffer[String]] = group_rdd.map({
      case (key, it) => {
        val out_res_list: ListBuffer[String] = ListBuffer[String]()
        var scene_count_map:Map[String, Int] = Map[String, Int]()
        val like_scene = like_scene_list.value
        like_scene.split('|').foreach(str=>{
          scene_count_map += ("\\D+".r.findFirstIn(str).get)->("\\d+".r.findFirstIn(str).get.toInt)
        })
        val scene_list: Array[String] = like_scene.split('|').map(str=>("\\D+".r.findFirstIn(str).get))
        val validity = broad_validity.value
        var main_scene = ""
        //如果存在主场景则主场景为分割后的第一个tlog_detail
        if (like_scene.length > 0) main_scene = scene_list(0)
        val sorted_list: Array[Array[String]] = it.toArray.sortWith((arr1, arr2) => {
          arr1(1).toLong < arr2(1).toLong //按照data中的时间字段降序排列
        })
        var photo_time = 0L
        //控制快照时间
        var cur_scene_time = 0L
        //控制当前场景时间
        var photo_flag = false //控制是否要快照
        //构造一个场景map
        var AAA_status_map: Map[String, Array[String]] = Map[String, Array[String]]()
        //给场景填充默认值
        scene_list.foreach(scene => AAA_status_map += scene -> Array())
        sorted_list.foreach(arr => {
          val cur_scene_name = arr(2)
          cur_scene_time = arr(1).toLong //当前场景时间
          //如果要快照并且快照时间不为0并且当前场景时间大于控制快照时间
          if (photo_flag && photo_time != 0 && cur_scene_time > photo_time) {
            var final_str = ""
            scene_list.foreach(scene => {
              val dataArr: Array[String] = AAA_status_map.getOrElse(scene, Array())
              val count=scene_count_map.getOrElse(scene, 0)
              //如果触发了快照，game_detail_all快照不到数据,用-1填写默认值
              if (dataArr.isEmpty){
                final_str+= Array.iterate("-1",count)(_ => "-1").mkString("|") + "|"
              }else {
                val data: String =dataArr(3)
                val time_diff = photo_time - dataArr(1).toLong
                if (time_diff <= validity) {
                  final_str += data.replaceAll("\\###", "|") + "|"
                } else {
                  final_str+= Array.iterate("-1",count)(_ =>"-1").mkString("|") + "|"
                }
              }
            })
            photo_flag = false
            out_res_list += final_str.substring(0, final_str.length - 1)
          }
          //向map里面填充数据
          AAA_status_map += cur_scene_name -> arr
          //如果当前场景=主场景更新控制快照时间和快照标记
          if (cur_scene_name == main_scene) {
            photo_time = cur_scene_time
            photo_flag = true
          }
        })

        //收尾
        if (photo_time == cur_scene_time) {
          var final_str = ""
          scene_list.foreach(scene => {
            val dataArr: Array[String] = AAA_status_map.getOrElse(scene, Array())
            val count=scene_count_map.getOrElse(scene, 0)
            //如果触发了快照，game_detail_all快照不到数据,用-1填写默认值
            if (dataArr.isEmpty){
              final_str+= Array.iterate("-1",count)(_ => "-1").mkString("|") + "|"
            }else {
              val data: String =dataArr(3)
              val time_diff = photo_time - dataArr(1).toLong
              if (time_diff <= validity) {
                final_str += data.replaceAll("\\###", "|") + "|"
              } else {
                final_str+= Array.iterate("-1",count)(_ =>"-1").mkString("|") + "|"
              }
            }
          })
          photo_flag = false
          out_res_list += final_str.substring(0, final_str.length - 1)
        }
        out_res_list
      }
    })
    val array: Array[ListBuffer[String]] = result_rdd.collect()
    val res: RDD[String] = result_rdd.flatMap(list=>
      list)
    res.foreach(println)

//    class SceneAcc extends AccumulatorV2[Map[String,Long],Map[String,Long]]{
//      private var _map:Map[String,Long] =Map[String,Long]()
//
//      override def isZero: Boolean = _map.isEmpty
//
//      override def copy(): AccumulatorV2[Map[String, Long], Map[String, Long]] = {
//        val acc = new SceneAcc
//        acc._map=this._map
//        acc
//      }
//
//      override def reset(): Unit = this._map=Map[String,Long]()
//
//      override def add(v: Map[String, Long]): Unit = {
//
//
//      }
//
//      override def merge(other: AccumulatorV2[Map[String, Long], Map[String, Long]]): Unit = ???
//
//      override def value: Map[String, Long] = ???
//    }

    //    result_rdd.foreach(list=>{
    //      list.foreach(str=>str)
    //    })
    //
    //
//    array.foreach(list => {
//      val result: RDD[String] = sc.parallelize(list)
//      result.foreach(print)
//    })





//    val res: RDD[ListBuffer[String]] = sc.parallelize(array)
//
//
//
//    result_rdd
//      .foreach(println)





































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
