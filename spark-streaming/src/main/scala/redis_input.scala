

  import java.text.SimpleDateFormat
  import java.util.Date

  import Util.RedisUtil
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.streaming.{Seconds, StreamingContext}

  import scala.collection.JavaConversions._
  import collection.JavaConverters._
  import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
  import org.json4s.jackson.JsonMethods
  import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Response}

  import scala.collection.mutable

  /**
    * ssd22.tsy.gamesafe.db:50022> SMEMBERS tsy_tableJoin_gameid_set
    * 1) "10000"
    * 2) "201"
    * 3) "23"
    * 4) "28"
    * 5) "3"
    * 6) "5"
    * ssd22.tsy.gamesafe.db:50022> SMEMBER tsy_tableJoin_keyType_set
    * (error) ERR unkown command 'SMEMBER' or wrong number of arguments
    * ssd22.tsy.gamesafe.db:50022> SMEMBERS tsy_tableJoin_keyType_set
    * 1) "gameid_policy"
    * 2) "policy"
    * 3) "qq"
    * 4) "uin"
    * 5) "work"
    */


object redis_input {

    def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setMaster("local[2]").setAppName("OrderApp")
      val sc=new SparkContext(conf)
      sc.setLogLevel("ERROR")
      val ssc = new StreamingContext(sc,Seconds(5))
      val input_ds: ReceiverInputDStream[String] = ssc.socketTextStream("9.134.217.5",9999)


  val REDIS_HOST="9.134.217.5"
  val REDIS_PORT=6379
  val REDIS_TIMEOUT=10000

//  val REDIS_PASSWD="redisqTwhoCn"



  def getNowDate():String={
    var now:Date = new Date()
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var today = dateFormat.format( now )
    today
  }


      //定义输入宏配置
      val GAME_ID="23"
      val PRO_NAME="wideTable"
      val MAIN_KEY_NAME="uin"
      val MAIN_KEY_POS="1"
      val SCENE="lol_game_detail"
      val EXPIRE_TIME="600"
      val IS_PHOTO="1"
      val IS_RESET="0"
      val IS_NULL_VALUE_CTRL="0"
      val FOREIGH_KEY_STR="none"
      val DATA_FIELD_INFO="account_id STRING zh"
      val FLOW_ID="123"
      val FLOW_OWNER="aresyhzhang"

  //
  def status_table_maintain(input_data: DStream[String]) = {
    val output_data = input_data.mapPartitions(x => {
      val config = new JedisPoolConfig()
      var result = List[String]()
      val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT)
      val jedis = pool.getResource()
//      val pool=RedisUtil.
//      val jedis: Jedis = RedisUtil.getJedisClient

      try {
        val pipeline = jedis.pipelined()

        //查询结果缓存
        var pip_res_map = mutable.Map[String, Response[java.util.Map[String,String] ] ]()

        //首先提取场景列表通用名称
        //23_uin_set
        val mainkey_scene_list = jedis.smembers(GAME_ID+"_"+MAIN_KEY_NAME+"_set")
        //1) "23_uin_lol_game_detail"
        //2) "23_uin_lol_kda



        //基础信息入库redis与PL交互
        //23_uin_lol_game_detail_info
        val scene_info_dict_name = GAME_ID+"_"+MAIN_KEY_NAME+"_"+SCENE+"_info"
        pipeline.hset(scene_info_dict_name, "KEY_GAME_ID", GAME_ID)
        pipeline.hset(scene_info_dict_name, "KEY_PRO_NAME", PRO_NAME)
        pipeline.hset(scene_info_dict_name, "KEY_MAIN_KEY_NAME", MAIN_KEY_NAME)
        pipeline.hset(scene_info_dict_name, "KEY_MAIN_KEY_POS", MAIN_KEY_POS)
        pipeline.hset(scene_info_dict_name, "KEY_SCENE", SCENE)
        pipeline.hset(scene_info_dict_name, "KEY_EXPIRE_TIME", EXPIRE_TIME)
        pipeline.hset(scene_info_dict_name, "KEY_IS_PHOTO", IS_PHOTO)
        pipeline.hset(scene_info_dict_name, "KEY_FOREIGH_KEY_STR", FOREIGH_KEY_STR)
        pipeline.hset(scene_info_dict_name, "KEY_DATA_FIELD_INFO", DATA_FIELD_INFO)

        /////////////////////
        pipeline.hset(scene_info_dict_name, "KEY_FLOW_ID", FLOW_ID)
        pipeline.hset(scene_info_dict_name, "KEY_FLOW_OWNER", FLOW_OWNER)
        //pipeline.hset(scene_info_dict_name, "KEY_UPDATE_TIME", getNowDate())
        //把游戏id和账号类型存储下来
        pipeline.sadd("tsy_tableJoin_gameid_set", GAME_ID)
        pipeline.sadd("tsy_tableJoin_keyType_set", MAIN_KEY_NAME)

        val bacth_num = 50
        var count = 0
        x.foreach(line => {
          count += 1
          //时间更新专门放在foreach做，可以监控数据有无
          if (count <= 3){
            pipeline.hset(scene_info_dict_name, "KEY_UPDATE_TIME", getNowDate())
          }

          //1534280186|31|2020-05-26 08:18:48|1841|88221320|4|272|711|1341|0|901|0|1|10854|11687|0|0|6848|15154|1|-12|21833|27661|1836|30|8348|0|5|55885|678778|138113
          //对数据按照|进行切割，保留最后一个数字
          val line_list = line.split("\\|", 0)

          //获取到主键方便命名
          val mainkey = line_list(MAIN_KEY_POS.toInt - 1)
          //同一类型集合名字构造
          //type_status_table_name：23_uin_lol_game_detail
          val type_status_table_name = GAME_ID+"_"+MAIN_KEY_NAME+"_"+SCENE
          //关联子宽表使用字典存储，在此构造字典的名字
          //23_uin_lol_game_detail_1534280186
          val sub_dict_name = GAME_ID+"_"+MAIN_KEY_NAME+"_"+SCENE+"_"+ mainkey



          //删除所有其他场景的数据
          /**
            * mainkey_scene_list:
            * 1) "23_uin_lol_game_detail"
            * 2) "23_uin_lol_kda"
            * mainkey:
            * 1534280186
            */
          //如果需要清空状态
          if (IS_RESET == "1") {
            for (item <- mainkey_scene_list.asScala) {
              //主动删除其它场景的数据
              //会把其他场景对应的qq号也删除，比如kda和detail
              //${GAME_ID}_${MAIN_KEY_NAME}_set -> 23_uin_lol_game_detail
              //${GAME_ID}_${MAIN_KEY_NAME}_set -> "23_uin_lol_kda"
              val stringArray = Array[String](item + "_" + mainkey)
              pipeline.del(stringArray:_*)
            }
          }

          //关联子宽表维护
          //sub_dict_name ${GAME_ID}_${MAIN_KEY_NAME}_${SCENE}_
          //23_uin_lol_game_detail_all_1534280186
          //1534280186|31|2020-05-26 08:18:48|1841|88221320|4|272|711|1341|0|901|0|1|10854|11687|0|0|6848|15154|1|-12|21833|27661|1836|30|8348|0|5|55885|678778|138113
          //判断是否进行空值更新，
          /**
            * 循环去找传入的字符串中是否有null，NULL," "
            * 存的是第三层的数据，也就是传入的每行数据
            */
          if (IS_NULL_VALUE_CTRL == "1") {
            val null_val: List[String] = List("null", "NULL", "")
            for (i <- 1 to line_list.length) {
              if (!(null_val.contains( line_list(i-1) ) ) ){
                //??????????
                //构造关联子宽表 字典的kv并插入字典 ，key使用场景名+列号，value使用具体的值
                pipeline.hset(sub_dict_name, SCENE+"_"+ i, line_list(i-1)) //开始下标为1
              }
            }
          }
          else{
            for (i <- 1 to line_list.length) {
              //构造关联子宽表 字典的kv并插入字典 ，key使用场景名+列号，value使用具体的值
              pipeline.hset(sub_dict_name,  SCENE+"_" + i, line_list(i-1)) //开始下标为1
            }
          }


          //主键信息存储
          pipeline.hset(sub_dict_name, "main_key", mainkey)
          //外键信息存储
          pipeline.hset(sub_dict_name, "foreign_key", FOREIGH_KEY_STR)
          //场景信息存储
          pipeline.hset(sub_dict_name, "scene", SCENE)
          //key类型信息存储
          pipeline.hset(sub_dict_name, "keyType", MAIN_KEY_NAME)
          //当前数据的长度
          pipeline.hset(sub_dict_name, "attrCnt", line_list.length.toString)
          //key创建&更新以来的次数统计
          pipeline.hincrBy(sub_dict_name, "updateCnt", 1)
          //关联子宽表的过期时间设置，当前数据的超时设置，超过这个时间则认为没有这个场景
          pipeline.expire(sub_dict_name, EXPIRE_TIME.toInt)

          /**
            * type_status_table_name="${GAME_ID}_${MAIN_KEY_NAME}_${SCENE}
            * sub_dict_name="${GAME_ID}_${MAIN_KEY_NAME}_${SCENE}_" + mainkey
            *
            */
          //单个key状态宽表维护，关联操作的核心，单个key状态宽表的关联结果，使用集合set存储
          pipeline.sadd(type_status_table_name, sub_dict_name)
          //单个key状态宽表有必要设置过期时间，三天不活跃的账号将不再维护
          pipeline.expire(type_status_table_name, 259200)
          //整个状态宽表的维护，也使用set。不设置过期时间，因为全局没几个可手动维护
          pipeline.sadd(GAME_ID+"_"+MAIN_KEY_NAME+"_"+"set", type_status_table_name)


          //如果需要生成快照，查询其它字典形成状态宽表一起发送到kafka
            if (IS_PHOTO == "1") {
            for (item <- mainkey_scene_list.asScala) {
              //得到其它场景的数据
              //item + "_" + mainkey =23_uin_lol_game_detail_1534280186=sub_dict_name
              val hgetAll_res = pipeline.hgetAll(item + "_" + mainkey)
              //加＃号是为了支持多对一  ？？？？？？？？？？？？
              pip_res_map += (item + "_" + mainkey + "#" + count -> hgetAll_res)
            }
          }


          /**
            * 如果达到50条数据，才使用pipeline一次性执行命令
            */
          if (count % bacth_num == 0) {
            //如果不生成快照，直接pipe执行
            if (IS_PHOTO == "1") {
              pipeline.sync()
              val pipe_exec_res = redis_pipe_exec(jedis, pipeline, pip_res_map)
              result = result:::pipe_exec_res
              pip_res_map.clear()
            }else{
              pipeline.sync()
            }
          }
        })
        //补刀执行pipeline
        if (IS_PHOTO == "1") {
          pipeline.sync()
          val pipe_exec_res = redis_pipe_exec(jedis, pipeline, pip_res_map)
          result = result:::pipe_exec_res
          pip_res_map.clear()
        }else{
          pipeline.sync()
        }
        pool.returnResource(jedis)
        pool.destroy()
      }
      catch {
        case e: Any => {
          pool.returnBrokenResource(jedis)
          println("=========jupite1========= " + getNowDate())
          println(e.printStackTrace())
//          println(e.getStackTrace.mkString("\n"))
        }
      }
      result.iterator
    })
    output_data
  }


  def redis_pipe_exec(jedis: redis.clients.jedis.Jedis, pipeline: redis.clients.jedis.Pipeline, pip_res_map: mutable.Map[String,Response[java.util.Map[String,String]]]):List[String] ={

    //map不可能为空，因为至少有当批次的数据，因此不作非空判断

    //外键关联最终数据汇总列表
    var foreign_key_data_list  = List[mutable.Map[String, Response[java.util.Map[String,String] ] ] ]()

    //避免重复查询多个相同外键
    var query_dict_name_list = List[String]()
    //避免多次执行redis查询
    var foreign_scene_dict = mutable.Map[String, List[String] ]()

    //用于控制循环
    var pipeline_res_isEmpty = false

    ///查询结果缓存

    //用于暂存进行循环
    var pip_res_map_tmp : mutable.Map[String, Response[java.util.Map[String,String] ] ] = pip_res_map.clone()
    //用于获取查询结果
    var pip_res_map_tmp2 = mutable.Map[String, Response[java.util.Map[String,String] ] ]()


    var count = 0

    do{
      count += 1
      try{
        //keys:23_uin_lol_game_detail_1534280186=sub_dict_name
        //
        pip_res_map_tmp.keys.foreach{
          i => {
            //初始被关联的主键获取 ??????
            val foreign_key_flag = i.split("#%#",0).head
            //得到当前场景存储的数据
            var item_dict = pip_res_map_tmp(i).get().asScala

            /**
              * lol_game_detail_1 ,1534280186
              * lol_game_detail_2 ,31
              * lol_game_detail_3 ,asdfs
              * lol_game_detail_4 ,2020-12:43
              * main_key->1534280186
              * updateCnt->1
              */
            println("=========jupite0714=========size: " + item_dict.size)
            println("=========jupite0714========= i: " + i)
            //如果当前场景有数据
            if (item_dict.size > 0){
              //当前层主键获取
              var main_key = item_dict("main_key")
              val scene = item_dict("scene")
              val foreign_key_str = item_dict("foreign_key")

              //?????
              if (foreign_key_str != "none"){
                //外键信息解析
                  val foreign_key_dict = foreign_key_str.split(",", 0).map(
                  x => {
                    val x_list = x.split(":", 0)
                    (x_list(0), x_list(1))
                  }
                ).toMap

                foreign_key_dict.keys.foreach {
                  key_name => {
                    //外键集合名构造
                    val foreign_set_name = GAME_ID + key_name + "_set"
                    //外键场景列表
                    var foreign_scene_list = List[String]()
                    //避免重复查询的逻辑
                    if (foreign_scene_dict.contains(foreign_set_name)){
                      foreign_scene_list = foreign_scene_dict(foreign_set_name)
                    }else{
                      /**
                        * foreign_scene_list的值：
                        * 23_uin_lol_kda
                        * 23_uin_lol_game_detail
                        */
                      foreign_scene_list = jedis.smembers(foreign_set_name).asScala.toList

                      /**
                        * foreign_scene_dict:
                        * 23_uin_set -> List((23_uin_lol_kda),(23_uin_lol_game_detail))
                        */
                      foreign_scene_dict += (foreign_set_name -> foreign_scene_list)
                    }
                    //外键信息查询
                    for( foreign_scene <- foreign_scene_list ){
                      //外键字典名构造
                      /**
                        * 23_uin_lol_kda _
                        * item_dict(lol_kda_)
                        */
                      val foreign_dict_name = foreign_scene+"_"+item_dict(scene+"_"+foreign_key_dict(key_name))
                      if(!query_dict_name_list.contains(foreign_dict_name+"_"+foreign_key_flag)){
                        //添加标记，以免重复查询
                        query_dict_name_list = foreign_dict_name+"_"+foreign_key_flag::query_dict_name_list
                        //得到其它场景的数据
                        val hgetAll_res = pipeline.hgetAll(foreign_dict_name)
                        pip_res_map_tmp2 += (foreign_key_flag + "#%#" + foreign_dict_name -> hgetAll_res)
                      }
                    }

                  }
                }
              }
            }

          }
        }
        //批量执行
        pipeline.sync()

      }
      catch {
        case e: Any => {
          println("=========jupite2========= " + getNowDate())
          println(e.getStackTrace.mkString("\n"))
        }
      }


      pipeline_res_isEmpty = pip_res_map_tmp2.isEmpty
      if (!pipeline_res_isEmpty){
        //结果暂存
        foreign_key_data_list = pip_res_map_tmp2.clone()::foreign_key_data_list
        //进行迭代
        pip_res_map_tmp.clear()
        pip_res_map_tmp = pip_res_map_tmp2.clone()
        //清空缓存
        pip_res_map_tmp2.clear()
      }


    }while (!pipeline_res_isEmpty)

    ////////////////////////////////////

    //此处进行最终整理
    var key_data_list_dict = mutable.Map[String, List[String] ]()
    pip_res_map.keys.foreach{
      i => {
        val main_key = i.split("_",0).last
        val cur_scene_data = pip_res_map(i).get().asScala.mkString("###")
        if ( key_data_list_dict.contains(main_key)){
          key_data_list_dict(main_key) = cur_scene_data::key_data_list_dict(main_key)
        }else{
          key_data_list_dict += (main_key -> List(cur_scene_data))
        }

      }
    }


    //外键关联补充上

    for( foreign_layer_dict <- foreign_key_data_list){
      for( foreign_key <- foreign_layer_dict.keys){
        val main_key_str = foreign_key.split("#%#",0).head.split("_",0).last
        val foreign_scene_data = foreign_layer_dict(foreign_key).get().asScala.mkString("###")
        if ( key_data_list_dict.contains(main_key_str)){
          key_data_list_dict(main_key_str) = foreign_scene_data::key_data_list_dict(main_key_str)
        }else{
          key_data_list_dict += (main_key_str -> List(foreign_scene_data))
        }
      }

    }


    key_data_list_dict.toList.map(t=>t._2.mkString("#@#"))

  }

  val outputDS = status_table_maintain(input_ds)

  outputDS.print()
      ssc.start()
      ssc.awaitTermination()
    }

}
