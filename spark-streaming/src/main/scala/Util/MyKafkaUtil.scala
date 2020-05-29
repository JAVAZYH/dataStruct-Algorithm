//package Util
//
///**
//  * @ Author     ：javazyh.
//  * @ Date       ：Created in 2019-12-${DAT}-15:54
//  * @ Description：${description}
//  * @ Modified By：
//  *
//  * @Version: $version$
//  */
//import kafka.serializer.StringDecoder
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//
///**
//  * Author lzc
//  * Date 2019/5/15 11:19 AM
//  */
//object MyKafkaUtil {
//
//    def getKafkaStream(ssc: StreamingContext, topic: String): InputDStream[(String, String)] = {
//        val params: Map[String, String] = Map(
//            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getProperty("config.properties","kafka.broker.list"),
//            ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtil.getProperty("config.properties","kafka.group")
//        )
//        KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,params,Set(topic))
//    }
//}
