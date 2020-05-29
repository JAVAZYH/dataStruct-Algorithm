//package Util
//
//import org.apache.spark.streaming.dstream.DStream
//
///**
//  * @ Author     ：javazyh.
//  * @ Date       ：Created in 2019-12-${DAT}-9:51
//  * @ Description：${description}
//  * @ Modified By：
//  *
//  * @Version: $version$
//  */
//object HBaseUtil {
//
//    //使用phoenix把数据写入到HBase
//    def saveToHBase[T <:Product](sourceDStream: DStream[T],
//                                 tableName:String,
//                                 fileNames:Seq[String]
//                                ,zkUrl:Some[String]): Boolean ={
//        if(sourceDStream!=null && tableName!=null && tableName!="" && fileNames.nonEmpty && zkUrl!=null ){
//            import org.apache.phoenix.spark._
//            sourceDStream.foreachRDD(rdd=>{
//                rdd.saveToPhoenix(tableName,fileNames,zkUrl=zkUrl)
//            })
//            true
//        }else{
//            //抛出自定义异常
//            false
//        }
//    }
//
//    //使用phoenix从HBase中读取数据
//
//
//}
