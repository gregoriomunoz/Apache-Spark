package learning.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ApplicationStepByStep {

	public static void main(String[] args) {

		// Create a Java Spark Context
		final SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		final JavaSparkContext sparkContext = new JavaSparkContext(conf);

		// Load out input data
		final JavaRDD<String> input = sparkContext.textFile("src/main/resources/el-quijote-capitulo-1.txt");
		
		// Split up into words
		final JavaRDD<String> words = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		
		// Trasnform into pairs and count
		final JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
		final JavaPairRDD<String, Integer> counts = pairs.reduceByKey((x,y) -> x+y);

		// print results
		final List<Tuple2<String, Integer>> output = counts.collect();
		
		output.stream().forEach(System.out::println);
		
		sparkContext.stop();
		sparkContext.close();
	}

}
