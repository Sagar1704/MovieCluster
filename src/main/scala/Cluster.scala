package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Cluster {
    val k = 10
    val maxIterations = 20
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("movieCluster")
        val sc = new SparkContext(conf)

        if (args.length != 2) {
            println("Usage: [itemusermat] [movies]")
        } else {
            //        val itemUserMatrix = sc.textFile("S:\\Spring2016\\BigData\\Homeworks\\Homework3\\dataset\\itemusermat")
            val itemUserMatrix = sc.textFile(args(0))

            val data = itemUserMatrix.map(line => Vectors.dense(line.split(" ").drop(1).map(_.toDouble))).cache()
            val kMeansModel = KMeans.train(data, k, maxIterations)

            val movieRatings = itemUserMatrix.map(line => getMovieRatings(line)).map(item => (item._1, Vectors.dense(item._2.map(_.toDouble))))

            val clusters = movieRatings.map(item => (item._1, kMeansModel.predict(item._2)))

            //        val moviesData = sc.textFile("S:\\Spring2016\\BigData\\Homeworks\\Homework3\\dataset\\movies.dat")
            val moviesData = sc.textFile(args(1))
            val movies = moviesData.map(line => line.split("::")).map(item => (item(0).toLong, item.mkString(",")))

            val moviesCluster = movies.join(clusters).map(item => item.swap).map(item => (item._1._2, item._1._1)).reduceByKey(_ + "%" + _)
            val output = moviesCluster.map(item => (item._1, item._2.split("%").mkString("\n\t"))).sortBy(_._1, true).map(item => ("Cluster" + item._1 + "\n\t" + item._2))

            output.saveAsTextFile(".\\output")
        }
    }

    def getMovieRatings(line: String): (Long, Array[String]) = {
        val movieRatings = line.split(" ")
        val movieID = movieRatings(0).toLong

        val ratings = movieRatings.drop(1)

        return (movieID, ratings)
    }

}