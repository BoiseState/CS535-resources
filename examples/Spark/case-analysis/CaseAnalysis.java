
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
import java.util.Map;

public final class CaseAnalysis
{
    public static void main(String[] args) throws Exception {

	if (args.length < 1) {
	    System.err.println("Usage: use spark-submit command!");
	    System.exit(1);
	}

	SparkConf conf = new SparkConf().setAppName("CaseAnalysis");
	JavaSparkContext sc = new JavaSparkContext(conf);

	JavaRDD<String> lines = sc.textFile(args[0]);
	JavaRDD<String> letters = lines.flatMap(s -> Arrays.asList(s.split("")).iterator());
	long n = letters.count();
	System.out.println("count = " + letters.count());

	JavaPairRDD<String, Integer> ones = letters.mapToPair(s -> {
	    Tuple2<String, Integer> t = new Tuple2<String, Integer>("other", 1);
	    if (s.length() > 0) {
		if (Character.isUpperCase(s.charAt(0)))
		    t = new Tuple2<>(s.toUpperCase(), 1);
		else if (Character.isLowerCase(s.charAt(0))) t = new Tuple2<>(s.toUpperCase(), 0);
	    }
	    return t;
	});

	JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
	// counts.saveAsTextFile("hdfs://localhost:9000/user/amit/output");
	counts.saveAsTextFile("output");
	List<Tuple2<String, Integer>> output = counts.collect();
	for (Tuple2<?, ?> tuple : output) {
	    System.out.println(tuple._1() + ": " + tuple._2());
	}

	Map<String, Long> totals = ones.countByKey();
	totals.forEach((key, value) -> System.out.println(key + " " + value));

	sc.stop();
	sc.close();
    }
    
    public static void printRDD(JavaPairRDD<Integer, Integer> rdd) {
		List<Tuple2<Integer, Integer>> output = rdd.collect();
		System.out.println();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println("(" + tuple._1() + "," + tuple._2() + ")");
		}
		System.out.println();
	}
}
