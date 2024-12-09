package org.example.scala.pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.File
import java.util.Date

object PageRank {
  def run(args: Array[String]): Unit = {
    // 步骤1：通过SparkConf设置配置信息，并创建SparkContext
    val conf = new SparkConf
    conf.setAppName("PageRank")
    // conf.setMaster("local") // 仅用于本地进行调试，如在集群中运行则删除本行
    val sc = new SparkContext(conf)

    // 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等
    val iterateNum = 20 // 指定迭代次数
    val factor = 0.85 // 阻尼系数

    //    val text = sc.textFile("inputFilePath")
    //    System.out.println(args(0))
    //    System.out.println(args(1))
    // 读取输入文本数据
    val text = sc.textFile(args(0))

    //    System.out.println("==================================")

    // 将文本数据转换成[网页, {链接列表}]键值对
    val links = text.map(line => {
      val tokens = line.split(" ")
      var list = List[String]()
      for (i <- 2 until tokens.size by 2) {
        list = list :+ tokens(i)
      }
      (tokens(0), list)
    }).cache() // 持久化到内存

    // 网页总数
    val N = 10
    //    System.out.println(N)

    //初始化每个页面的排名值，得到[网页, 排名值]键值对
    var ranks = text.map(line => {
      val tokens = line.split(" ")
      (tokens(0), tokens(1).toDouble)
    })

    // 执行iterateNum次迭代计算
    for (iter <- 1 to iterateNum) {
      val contributions = links
        // 将links和ranks做join. 得到[网页, {{链接列表}, 排名值}]
        .join(ranks)
        // 计算出每个网页对其每个链接网页的贡献值 = 网页排名值 / 链接总数
        .flatMap {
          case (page, (links, rank)) =>
            links.map(dest => (dest, rank / links.size))
        }

      ranks = contributions
        // 聚合对相同网页的贡献值，求和得到对每个网页的总贡献值
        .reduceByKey(_+_)
        // 根据公式计算得到每个网页的新排名值
        .mapValues(v => (1 - factor) * 1.0 / N + factor * v)
    }

    // 对排名值保留5位小数，并打印最终网页排名结果
    ranks.foreach(t => println(t._1 + " ", t._2.formatted("%.5f")))
    ranks.saveAsTextFile(args(1))

    // 步骤3：关闭SparkContext
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    // 步骤4：运行作业，并计算运行时间
    var startTime =new Date().getTime
    run(args)
    var endTime =new Date().getTime
    println("programme runtime: " + (endTime - startTime) + "ms") //单位毫秒
  }
}

/*
  与MapReduce网页链接排名的实现相比，MapReduce程序每次迭代结束时会将本次迭代的结果写入HDFS，下一次迭代再从HDFS中读入上一次迭代的结果，
  反复读写HDFS的开销大。而Spark程序每一次迭代得到一个RDD，该RDD可以住存在内存中作为下一次迭代的输入，避免了冗余的读写开销。此外，对于在
  迭代过程中保持不变的静态数据（例如，网页链接排名中的网页链接数据），Spark可以利用持久化机制将其缓存在内存中，从而避免冗余加载或冗余计算。
  因此，Spark在迭代计算方面性能优于MapReduce。
 */

