package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Cluster {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("movieCluster")
        val sc = new SparkContext(conf)

        val input = sc.textFile(args(0))
    }
}