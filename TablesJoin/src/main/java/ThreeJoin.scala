import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ThreeJoin {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("test").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val input_rdd_1: RDD[String] = sc.textFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\test1.txt")
    val input_rdd_2: RDD[String] = sc.textFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\test2.txt")
    val input_rdd_3: RDD[String] = sc.textFile("C:\\Users\\aresyhzhang\\Desktop\\临时\\test3.txt")



//    val sqlStr=
//      s"""
//         |select * from tlog_deail_df a
//         |left join wegame_detail_df b
//         |on a.iuin=b.iuin and a.worldid=b.world_id and a.igameseq=b.game_id
//         |left join keypress_df c
//         |on a.iuin=c.real_uin and a.worldid=c.ssn and a.igameseq=c.teamid
//       """.stripMargin
//
//
//    val key_str="iuin|worldid|battleid"
//
//    val data_str=""


//    val input_rdd_1=Seq("2020042212|3|746|6274584572|1534280186|2020-04-22 12:42:54|2020-04-22 12:18:12|11|NULL|1482|NULL|NULL|0|3|1|2|0|589|0|0|0|0|62764|11851|10909|2965|6|1|0|3489|57|0|62764|8362|38|5|6|7|32|498|0.0|false|0|0|0|0|0|0|0|29|830|0|0|0|0|1587528765207|1587528766271|1587528834385")
//     val input_rdd_2=Seq("2020042222|3|1|3|11|2020-04-22 22:32:46|200218378|350|100|8|0|8|1|0|0|9224192|0|2|255|255|1534280186|3|200218378")

//    val groupRDD1 = input_rdd_1.map(line => {
//      line.split('|')
//      val qq = line(5)
//      val worldid = line(2)
//      val battleid = line(14)
//      (qq + "|" + worldid + "|" + battleid, line)
//    }).groupByKey()
//    val groupRDD2= input_rdd_2.map(line => {
//      line.split('|')
//      val qq = line(21)
//      val worldid = line(22)
//      val battleid = line(4)
//      (qq + "|" + worldid + "|" + battleid, line)
//    }).groupByKey()


    val list = List("1534280186|1|1","1|1|1")
    val keys: Broadcast[List[String]] = sc.broadcast(list)

   val groupRDD1 = input_rdd_1.map(line => {
        val strings: Array[String] = line.split('|')
        val qq = strings(0)
        val worldid = strings(1)
        val battleid = strings(2)
        (qq + "|" + worldid + "|" + battleid, line)
      })

      val groupRDD2= input_rdd_2
//        .filter(line=>{
//        val keylist: List[String] = keys.value
//        val strings: Array[String] = line.split('|')
//        val qq = strings(0)
//        val worldid = strings(1)
//        val battleid = strings(2)
//        keylist.contains(qq + "|" + worldid + "|" + battleid)
//      })
        .map(line => {
        val keylist: List[String] = keys.value
        val strings: Array[String] = line.split('|')
        val qq = strings(0)
        val worldid = strings(1)
        val battleid = strings(2)
        val key=qq + "|" + worldid + "|" + battleid
        if (keylist.contains(key)) key->line else key->"null"
      })

    val groupRDD3= input_rdd_3.map(line => {
      val strings: Array[String] = line.split('|')
      val qq = strings(0)
      val worldid = strings(1)
      val battleid = strings(2)
      (qq + "|" + worldid + "|" + battleid, line)
    })

    val tlog_wegame_rdd: RDD[(String, (Iterable[String], Iterable[String]))] = groupRDD1.cogroup(groupRDD2)

    val tlog_wegame_kv: RDD[(String, String)] = tlog_wegame_rdd.map({
      case (key, (it1, it2)) => {
        (key, it1.mkString("|") + "|" + it2.mkString("|"))
      }
    })
    val tlog_wegame_keypress_rdd: RDD[(String, (Iterable[String], Iterable[String]))] = tlog_wegame_kv.cogroup(groupRDD3)

    val resultRDD: RDD[(String, String)] = tlog_wegame_keypress_rdd.map({
      case (key, (it1, it2)) => {
        (key, {
          if (it2==null || it2.isEmpty){it1.mkString("#")}
          else {it1.mkString("|") + "|" + it2.mkString("|")}})
      }
    })





//    def rddTokv(rdd1:RDD[(String, (Iterable[String], Iterable[String]))],rdd2:RDD[(String, String)])={
//
//    }
//
//    def createValue(it1:Iterable[String],it2:Iterable[String]) ={
//     if (it1==null || it1.isEmpty){it2.mkString("|")}
//      if (it2==null || it2.isEmpty){it1.mkString("|")}
//      else {it1.mkString("|") + "|" + it2.mkString("|")}
//    }

//    val resultRDD: RDD[(String, String)] = groupRDD1.union(groupRDD2)


//      groupRDD1
//      .cogroup(groupRDD2, groupRDD3,groupRDD3)
//
////    value
////    val value: RDD[(String, (Iterable[Iterable[String]], Iterable[Iterable[String]]))] = groupRDD1
////      .cogroup(groupRDD2)
//
//    val value: RDD[(String, (Iterable[(Iterable[String], Iterable[String])], Iterable[String]))] = groupRDD1
//      .cogroup(groupRDD2).cogroup(groupRDD3)



//      .map({
//        case (key, (it1, it2)) => {
//         key+"|"+ it1.mkString("|") +"|"+ it2.mkString("|")
//        }
//      })





    tlog_wegame_kv.foreach(println)



//    groupRDD1.zipPartitions()




  }
}
