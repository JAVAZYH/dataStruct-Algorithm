
import java.util.logging.SimpleFormatter
import java.text.SimpleDateFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object tsyPre {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("test").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val input_rdd_1: RDD[String] = sc.textFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\测试数据\\keypress.txt")



//    val uin_pos="5|2|14"
//    val time_pos="0"
//    val scene_name="tlog_detail"

//        val uin_pos="21|22|4"
//        val time_pos="0"
//        val scene_name="wegame_detail"

        val uin_pos="26|3|4"
        val time_pos="0"
        val scene_name="keypress"

    val broad_uin_pos: Broadcast[String] = sc.broadcast(uin_pos)
    val broad_time_pos: Broadcast[String] = sc.broadcast(time_pos)
    val broad_scene_name: Broadcast[String] = sc.broadcast(scene_name)
    val res: RDD[String] = input_rdd_1.map(line => {
      val uin_pos = broad_uin_pos.value
      val time_pos = broad_time_pos.value
      val scene_name = broad_scene_name.value
      val line_info: Array[String] = line.split('|')
      val uin_pos_list: Array[String] = uin_pos.split('|')
      var main_key = ""
      uin_pos_list.foreach(str => {
        main_key += line_info(str.toInt - 1) + "_"
      })
//      //截去最后一个_
//      main_key = main_key.substring(0, main_key.length - 1)
      var second_key = ""
      if (time_pos == "0") {
        second_key = "10000"
      }
      else {
        try{
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val timeStamp: Long = format.parse(line_info(time_pos.toInt-1)).getTime/1000
          second_key = timeStamp.toString
        }catch {
          case e:Exception=>{
            second_key= "10000"
          }
        }
      }
      main_key + "|" + second_key + "|" + scene_name + "|" + line.replaceAll("\\|", "###")
//      main_key + "|" + second_key + "|" + scene_name + "|" + line
    })
//    res.collect()
//    res.saveAsTextFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\测试数据\\all.txt")
    res.foreach(println)
  }
}

//    class TimeUtil extends Serializable{
//      def time_convert(input_time: String): Long ={
//        val format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//        val timeStamp: Long = format.parse(input_time).getTime
//        timeStamp
//      }
//    }