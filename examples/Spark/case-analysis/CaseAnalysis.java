
/** 
 * @author: amit 
 **/

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class CaseAnalysis {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file or folder>");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("CaseAnalysis");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile(args[0]);
		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		counts.saveAsTextFile("hdfs://localhost:9000/user/amit/output");

		// List<Tuple2<String, Integer>> output = counts.collect();
		// for (Tuple2<?, ?> tuple : output) {
		// System.out.println(tuple._1() + ": " + tuple._2());
		// }
		sc.stop();
		sc.close();
	}
}
