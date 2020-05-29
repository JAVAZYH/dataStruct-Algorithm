//package Util
//
//import io.searchbox.client.JestClientFactory
//import io.searchbox.client.config.HttpClientConfig
//import io.searchbox.core.{Bulk, Index}
//import org.apache.spark.rdd.RDD
//
///**
//  * @ Author     ：javazyh.
//  * @ Date       ：Created in 2019-12-${DAT}-14:31
//  * @ Description：${description}
//  * @ Modified By：
//  *
//  * @Version: $version$
//  */
//object ESUtil {
//    //1. es客户端
//    val factory = new JestClientFactory
//    val esUrl="http://hadoop102:9200"
//    val conf =
//        new HttpClientConfig
//        .Builder(esUrl)
//            .maxTotalConnection(100)
//            .multiThreaded(true)
//            .connTimeout(10000)
//            .readTimeout(10000)
//            .build()
//    factory.setHttpClientConfig(conf)
//
//    //获取客户端
//    def getClient() = factory.getObject
//
//    //插入单条记录
//    def insertSingle(indexName:String,source:Any): Unit ={
//        val client=getClient()
//        val action=new Index.Builder(source)
//            .index(indexName)
//            .`type`("_doc")
//            .build()
//        client.execute(action)
//        client.close()
//    }
//
//    /**
//      * ["user",[("1",user),(id,user)...]]
//      * ["user",[user,user]
//      * @param indexName
//      * @param sources
//      */
//    //批量插入多条记录
//    def insertBulk(indexName:String,sources:Iterator[Any]): Unit ={
//    val client=getClient()
//        val bulk=new Bulk.Builder()
//                .defaultIndex(indexName)
//                .defaultType("_doc")
//          sources.foreach({
//              case (id:String,source)=>{
//                  println(source)
//                  val action=new Index.Builder(source).id(id).build()
//                  bulk.addAction(action)
//              }
//              case source=>{
//                  val action=new Index.Builder(source).build()
//                  bulk.addAction(action)
//              }
//          })
//        client.execute(bulk.build())
//        client.shutdownClient()
//
//    }
//
////    def main(args: Array[String]): Unit = {
////        it
////        insertBulk("gmall_coupon_alert",)
////    }
//
//}
//
