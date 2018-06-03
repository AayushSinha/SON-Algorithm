import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.{Set => immutSet}
import scala.collection.mutable.LinkedHashMap
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io._

object Aayush_Sinha_problem2 {
  def main(args: Array[String]) {

    val start_time = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("Aayush_Sinha_Son").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val csvfile = args(1)
    val csv = sc.textFile(csvfile)
    val data = csv.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val casenum = args(0).toInt
    val Support = args(2).toInt
    var singleslist:Iterable[Set[String]]=null
    var singles=ListBuffer.empty[Set[String]]
    var baskets:RDD[Set[String]]=null

    if (casenum==1)
    {
      baskets = data.map(line => line.split(",")).map(c => (c(0), c(1))).groupByKey().map { case (check, v) => v.toSet }
      singleslist = csv.map(line => line.split(",")).map(c => (c(1), 1)).countByKey().filter({ case (c, l) => (l >= Support) })
        .map(c => Set(c._1))
      for (i <- singleslist)
      {
        singles+=i
      }
    }
    else if(casenum==2)
    {
      baskets = data.map(line => line.split(",")).map(c => (c(1), c(0))).groupByKey().map { case (check, v) => v.toSet }
      singleslist = csv.map(line => line.split(",")).map(c => (c(0), 1)).countByKey().filter({ case (c, l) => (l >= Support) })
        .map(c => Set(c._1))
      for (i <- singleslist)
      {
        singles+=i
      }
    }
    val numPartitions = baskets.getNumPartitions
    var map1 = baskets.mapPartitions(lists => new Apriori().MyApriori(singles, lists, Support/numPartitions))
      .map { case (a, b) => b }   .flatMap(a => (a) map { case (set) => (set, (set, 1)) })
    var reduce1 = map1.reduceByKey((a: (Set[String], Int), b: (Set[String], Int)) => (a._1, 1)).map { case (a, b) => b }
    var candidates = reduce1.map(b => b._1).collect().toList
    var map2 = baskets.flatMap(
      basket => {
        var counts = ListBuffer.empty[Tuple2[Set[String], Int]]
        for (cnt <- candidates) {
          if (cnt.subsetOf(basket)) {
            counts += Tuple2(cnt, 1)
          }
        }
        counts.toList
      })

    var reduce2 = map2.reduceByKey((x, y) => x + y)
    var final_freq = reduce2.filter(x => (x._2 >= Support))
      .map { case (a, b) => (a.size, List(a.toList.sorted)) }
      .reduceByKey((x: List[List[String]], y: List[List[String]]) => x ++: y)
      .map { case (a, b) => (a, sort(b)) }
      .sortBy(_._1)
      .map { case (a, b) => {
        var tmp = ListBuffer.empty[String]
        for (list <- b) {
          tmp += list.mkString("('","','","')")
        }
        tmp.toList
      }
      }
    val result = final_freq.map(list => list.mkString).collect()//.foreach(println)
    val outputfilename = "Aayush_Sinha_SON_"+csvfile.substring(csvfile.lastIndexOf("/")+1, csvfile.lastIndexOf("."))+".case"+ args(0) + "-"+args(2) +".txt"
    val fileoutput = new PrintWriter(new File(outputfilename))
    for (input1 <- result){
      fileoutput.write(input1)
    }
    fileoutput.close()

    val end_time = System.currentTimeMillis()
    //println("Time: " + (end_time - start_time) / 1000 + " secs")
  }

  def sort[A](coll: Seq[Iterable[A]])(implicit ordering: Ordering[A]) = coll.sorted

  class Apriori {

    def Combs(freqItemsets: List[Set[String]], check: Int, singles: Set[String]): List[Set[String]] = {
      var combinationSet = immutSet.empty[Set[String]]
      for (i <- freqItemsets) {
        for (singleItem <- singles) {
          var newset_items = immutSet() ++ i
          newset_items += singleItem
          if (newset_items.size == check) {
            var validList = newset_items.subsets(check - 1).toList
            var valid = true
            if (check > 2) {
              for (newSet <- validList) {
                if (!freqItemsets.contains(newSet)) {
                  valid = false
                }
              }
            }
            if (valid) {
              combinationSet += newset_items.toSet
            }
          }
        }
      }
      val v: List[Set[String]] = combinationSet.toList
      combinationSet.toList
    }

    def Freqs(candidates: List[Set[String]], baskets: List[Set[String]], Supportport: Double): List[Set[String]] = {
      var basketsSet = ListBuffer.empty[Set[String]]
      for (basket <- baskets) {
        basketsSet += basket
      }

      var ItemMap = LinkedHashMap.empty[Set[String], Int]

      for (i <- candidates) {
        for (basket <- baskets) {
          if (i.subsetOf(basket)) {
            if (ItemMap.contains(i)) {
              ItemMap(i) += 1
              // println("a",ItemMap)
            } else {
              ItemMap += (i -> 1)
            }
          }
        }
      }
      var itemsets = ListBuffer.empty[Set[String]]
      for (i <- ItemMap) {
        if (i._2 >= Supportport) {
          itemsets += i._1
        }
      }
      itemsets.toList
    }
    def MyApriori(singles: ListBuffer[Set[String]], iter: Iterator[Set[String]], Supportport: Int): Iterator[(Int, List[Set[String]])] = {
      var singleMap = LinkedHashMap.empty[String, Int]
      var baskets = ListBuffer.empty[Set[String]]
      var freqBuffer = ListBuffer.empty[Set[String]]
      var list_freq = List.empty[Set[String]]
      var output = LinkedHashMap.empty[Int, List[Set[String]]]
      var singletons = Set.empty[String]
      for (i<-singles)
      {
        singletons++=i
      }
      while (iter.hasNext) {
        val basket = iter.next()
        baskets += basket
      }
      var check = 2
      if (!output.contains(check - 1)) {
        output += (check - 1 -> singles.toList)
      }
      list_freq = singles.toList
      while (!list_freq.isEmpty) {
        list_freq = Combs(list_freq, check, singletons)
        var list_freqList = Freqs(list_freq, baskets.toList, Supportport)
        if (!list_freqList.isEmpty) {
          output += (check -> list_freqList)
        }
        list_freq = list_freqList
        check = check + 1
      }
      output.iterator
    }
  }
}
