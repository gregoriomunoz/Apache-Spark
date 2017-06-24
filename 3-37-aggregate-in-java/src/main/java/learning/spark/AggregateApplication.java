package learning.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class AggregateApplication {

	public static void main(String[] args) {

		// Create a Java Spark Context
		final SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		final JavaSparkContext sparkContext = new JavaSparkContext(conf);

		final JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4));

		final AvgCount initial = new AvgCount(0, 0);

		final Function2<AvgCount, Integer, AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
			public AvgCount call(final AvgCount a, final Integer x) throws Exception {
				a.total += x;
				a.num += 1;
				return a;
			}
		};

		final Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
			@Override
			public AvgCount call(final AvgCount a, final AvgCount b) throws Exception {
				a.total += b.total;
				a.num += b.num;
				return a;
			}
		};
		
		final AvgCount result = rdd.aggregate(initial, addAndCount, combine);
		System.out.println("Average is " + result.avg());
		
		sparkContext.stop();
		sparkContext.close();
	}

}
