package learning.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object App {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("My App");
    val sparkContext = new SparkContext(conf);

  }

}
