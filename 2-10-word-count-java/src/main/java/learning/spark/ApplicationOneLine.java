package learning.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ApplicationOneLine {

	public static void main(String[] args) {

		// Create a Java Spark Context
		final SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		final JavaSparkContext sparkContext = new JavaSparkContext(conf);

		sparkContext.textFile("src/main/resources/el-quijote-capitulo-1.txt")
				.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<String, Integer>(word, 1))
				.reduceByKey((x,y) -> x+y)
				.collect()
				.stream().forEach(System.out::println);;
		
		sparkContext.stop();
		sparkContext.close();
	}

}
