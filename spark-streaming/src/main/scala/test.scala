import java.util

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}
//import redis_input.{REDIS_HOST, REDIS_PASSWD, REDIS_PORT, REDIS_TIMEOUT}

object test {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName("test")
//      .setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
//    val ssc = new StreamingContext(sc,Seconds(5))
//
////    val intpu_ds: DStream[String] = ssc.textFileStream("file:\\C:\\Users\\aresyhzhang\\Desktop\\临时\\实时")
//    val input_ds: ReceiverInputDStream[String] = ssc.socketTextStream("9.134.217.5",9999)
//    input_ds.print(10)
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.sparkContext.setLogLevel("warn")


//    val REDIS_HOST="ssd22.tsy.gamesafe.db"
//    val REDIS_PORT=50022
//    val REDIS_TIMEOUT=10000
//    val REDIS_PASSWD="redisqTwhoCn"
//    val config = new JedisPoolConfig()
//    var result = List[String]()
//    val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT, REDIS_PASSWD)
//    val jedis = pool.getResource()
//
//    val main_keys: util.Set[String] = jedis.smembers("${GAME_ID}_${MAIN_KEY_NAME}_set")
//
//    main_keys.toArray.foreach(println)

//    println(1 % 50)

    val REDIS_HOST="9.134.217.5"
    val REDIS_PORT=6379
    val REDIS_TIMEOUT=10000
    val config = new JedisPoolConfig()
    val pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT)
    val jedis = pool.getResource()
    println(jedis.ping())



  }

}
