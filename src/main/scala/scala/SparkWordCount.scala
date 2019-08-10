package scala

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf()
    conf.setAppName("FirstApplication")
    conf.setMaster("local")
    val sc =new SparkContext(conf)
    //val input = sc.textFile("C:/Users/admin/Downloads/test.txt")
    //val input = sc.textFile("/home/sairavi/test.txt")
    System.out.println {
      args(0)+" First Step"
    }
    val input = sc.textFile(args(0))

    val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)
    //count.saveAsTextFile("/home/sairavi/wordcount/")
    count.saveAsTextFile(args(1))
      //val wordCounts = input.map((_, 1)).reduceByKey(_ + _)
    //val filtered = wordCounts.filter(_._2 >= threshold)
      //val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    System.out.println {
      "test 2"
    }


    //count.saveAsTextFile("/users/sairavi/wordcount/")
    System.out.println("OK")
  }

}
