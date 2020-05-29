//object redis_usedata {
//  // val inputDS_1: DStream[String]
//  // val inputDS_2: DStream[String]
//  //操作
//  //inputDS_1.print()
//  //
//
//  import java.util.{Calendar, Date}
//  import java.text.SimpleDateFormat
//  import redis.clients.jedis.{JedisPool, JedisPoolConfig, Response}
//  import scala.collection.JavaConversions._
//  import collection.JavaConverters._
//  import scala.collection.mutable.Map
//  import Array._
//
//  val REDIS_HOST="ssd22.tsy.gamesafe.db"
//  val REDIS_PORT=50022
//  val REDIS_TIMEOUT=10000
//  val REDIS_PASSWD="redisqTwhoCn"
//
//
//  def getNowDate():String={
//    var now:Date = new Date()
//    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    var today = dateFormat.format( now )
//    today
//  }
//
//  def status_table_maintain(input_data: DStream[String]) = {
//
//    val output_data = input_data.mapPartitions(x => {
//
//      val config = new JedisPoolConfig()
//      val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT, REDIS_PASSWD)
//      val jedis = pool.getResource()
//      val pipeline = jedis.pipelined()
//
//      //基础信息入库redis与PL交互
//      pipeline.sadd("tsy_wideTable_register_set", "${FLOW_ID}")
//      val data_info_dict_name = "tsy_wideTable_register_${FLOW_ID}_info"
//      pipeline.hset(data_info_dict_name, "KEY_FLOW_ID", "${FLOW_ID}")
//      pipeline.hset(data_info_dict_name, "KEY_GAME_ID", "${GAME_ID}")
//      pipeline.hset(data_info_dict_name, "KEY_PRO_NAME", "${PRO_NAME}")
//      pipeline.hset(data_info_dict_name, "KEY_FLOW_OWNER", "${FLOW_OWNER}")
//      pipeline.hset(data_info_dict_name, "KEY_INSTANCE_STARTER", "${INSTANCE_STARTER}")
//      pipeline.hset(data_info_dict_name, "KEY_UPDATE_TIME", getNowDate())
//      pipeline.hset(data_info_dict_name, "KEY_DATA_CNT", "0")
//      pipeline.hset(data_info_dict_name, "KEY_LIKE_SCENE", "${LIKE_SCENE}")
//      //补刀执行pipeline
//      pipeline.sync()
//
//      var result = List[String]()
//      try {
//        //原始订阅信息
//        val like_scene_list = "${LIKE_SCENE}".split("#_#", 0)
//        //单独保存订阅场景的字段个数信息
//        val like_scene_arrCnt_list = like_scene_list.map(x => {
//          val x_list = x.split("\\|",0)
//          x_list(2)
//        })
//        //单独保存订阅场景的稳定不变信息，方便字段扩展后关联
//        val like_scene_info_list = like_scene_list.map(x => {
//          val x_list = x.split("\\|",0)
//          val info_str = x_list(0) + "|" + x_list(1)
//          info_str
//        })
//        x.foreach(line => {
//          val line_list = line.split("\\|", 0)
//          val pro_name = line_list(3)
//          if ("${PRO_NAME}" == pro_name){
//            val photo_data = line_list(4)
//            val scene_data_list =photo_data.split("#@#", 0)
//            //输出列表构造
//            var photo_data_list_out:Array[String] = new Array[String](like_scene_info_list.size)
//            for (scene_data_str <- scene_data_list) {
//              if (scene_data_str != ""){
//                var scene_data_map  = scene_data_str.split("###",0).map(
//                  x => { val x_list = x.split(" -> ",-1)
//                    (x_list(0),x_list(1))
//                  }).toMap
//                var cur_scene = scene_data_map("scene")
//                var cur_keyType = scene_data_map("keyType")
//                //这个字段暂时没用了
//                var cur_attrCnt = scene_data_map("attrCnt")
//                //新增session统计字段
//                var cur_updateCnt = scene_data_map("updateCnt")
//                var type_scene_key = cur_keyType + "|" + cur_scene
//                val ts_key_index = like_scene_info_list.indexOf(type_scene_key)
//                if (like_scene_info_list.exists(x => x == type_scene_key )){
//                  scene_data_map-="main_key"
//                  scene_data_map-="scene"
//                  scene_data_map-="keyType"
//                  scene_data_map-="foreign_key"
//                  scene_data_map-="attrCnt"
//                  scene_data_map-="updateCnt"
//                  //首先创建默认规格的数组
//                  var padding_list:Array[String] = new Array[String](like_scene_arrCnt_list(ts_key_index).toInt)
//                  var padding_list_default = padding_list.map(x => "-1")
//                  //获取到当前场景数据的list
//                  //val cur_scene_list = scene_data_map.toSeq.sortBy( _._1.split("_",0).last.toInt).map(x => x._2)
//                  //安排到该有的格式中，建议每次新添加字段到最后，不是必须
//                  //cur_scene_list.copyToArray(padding_list_default)
//                  //遍历数据场景字典，将可能id不连贯的信息填充到结果list中
//                  scene_data_map.toSeq.sortBy( _._1.split("_",0).last.toInt).foreach(kv_i => {
//                    padding_list_default(kv_i._1.split("_",0).last.toInt - 1) = kv_i._2
//                  })
//
//                  //把额外session信息加入
//                  padding_list_default(padding_list_default.size - 1) = cur_updateCnt + ""
//                  photo_data_list_out(ts_key_index) = padding_list_default.mkString("|")
//                }
//              }
//            }
//            for ( i <- 0 to (photo_data_list_out.size - 1) ) {
//              if (photo_data_list_out(i) == null){
//                //没匹配到的算好空白安排进去
//                var padding_list:Array[String] = new Array[String](like_scene_arrCnt_list(i).toInt)
//                var padding_list_default = padding_list.map(x => "-1")
//                photo_data_list_out(i) = padding_list_default.mkString("|")
//              }
//            }
//            result = photo_data_list_out.mkString("|")::result
//          }
//
//        })
//      }
//      catch {
//
//        case e: Any => {
//          println("=========jupite0711=========")
//          println(e.getStackTrace.mkString("\n"))
//        }
//      }
//
//
//      //pool.returnResource(jedis)
//      pool.destroy()
//
//      result.iterator
//    })
//    output_data
//  }
//
//
//
//  val outputDS = status_table_maintain(inputDS_1)
//
//
//
//}
