package learning.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;

import learning.spark.model.Kv;


import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class Application implements Serializable {

	private static final long serialVersionUID = 7918507062795912997L;

	private transient SparkConf conf;

	private Application(SparkConf conf) {
		this.conf = conf;
	}

	private void run() {
		final JavaSparkContext sc = new JavaSparkContext(conf);
		generateData(sc);
		laodData(sc);
		sc.stop();
	}

	private void laodData(final JavaSparkContext sc) {
		final CassandraTableScanJavaRDD<CassandraRow> data = javaFunctions(sc).cassandraTable("test", "kv");
		System.out.println(data.mapToDouble(new DoubleFunction<CassandraRow>() {
			@Override
			public double call(CassandraRow row) throws Exception {
				return row.getInt("value");
			}
		}).stats());
	}

	private void generateData(final JavaSparkContext sc) {
		final CassandraConnector connector = CassandraConnector.apply(sc.getConf());
		
		// Prepare schema
		try (Session session = connector.openSession()) {
			session.execute("DROP KEYSPACE IF EXISTS test");
			session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE test.kv (key text PRIMARY KEY, value int)");
		}
		
		// Prepare Kvs
		final List<Kv> kvs = Arrays.asList(
				new Kv("k1",1),
				new Kv("k2", 2)
		);
		
		final JavaRDD<Kv> kvsRDD = sc.parallelize(kvs);
		javaFunctions(kvsRDD).writerBuilder("test", "kv", mapToRow(Kv.class)).saveToCassandra();
	}

	public static void main(String[] args) {

		if (args.length != 2) {
			System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
			System.exit(1);
		}

		// Create a Java Spark Context
		final SparkConf conf = new SparkConf().setMaster(args[0]).setAppName("My App")
				.set("spark.cassandra.connection.host", args[1]);

		// Create new Application
		final Application app = new Application(conf);
		app.run();

	}

}
