package allblue

import java.text.SimpleDateFormat

object test {
  def main(args: Array[String]): Unit = {
    //    val time1="2020-04-11 23:57:48"
    //    val time2="2020-04-11 23:50:48"
    //    println(time2long(time1) - time2long(time2))
    //
    //    def time2long(time:String) ={
    //      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //      val timeStamp: Long = format.parse(time).getTime/1000
    //      timeStamp
    //    }

    val input_column3="tmp,STRING"
    input_column3.split(',')(0) match {
      case "tmp"=>{
        println("111")
      }
      case _=>{
        println("222")
      }
    }


  }
}
