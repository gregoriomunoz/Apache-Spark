package learning.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Application {

	public static void main(String[] args) {

		// Create a Java Spark Context
		final SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		final JavaSparkContext sparkContext = new JavaSparkContext(conf);
		System.out.println(sparkContext);
	}

}
