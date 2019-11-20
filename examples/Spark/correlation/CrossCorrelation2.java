
/**
 *  
 *  Calculate cross correlation of a list of items.
 *  Given a set of tuples of items. For each possible pair of items 
 *  calculate the number of tuples where these items co-occur. 
 *  If the total number of items is n, then n^2 = n × n values are reported.
 *  Complete this example for exercise.
 *
 *  @author amit 
 *  
 */

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.List;
import java.util.regex.Pattern;

public final class CrossCorrelation2
{
    private static final Pattern SPACE = Pattern.compile("\\s+");


    public static void printPairRDD(JavaPairRDD<String, String> rdd) {
	List<Tuple2<String, String>> output = rdd.collect();
	System.out.println();
	for (Tuple2<?, ?> tuple : output) {
	    System.out.println("(" + tuple._1() + "," + tuple._2() + ")");
	}
	System.out.println();
    }
    
    public static void printTripletRDD(JavaPairRDD<Tuple2<String, String>, Integer>  rdd) {
    List<Tuple2<Tuple2<String, String>, Integer>> output = rdd.collect();
	System.out.println();
	for (Tuple2<?, ?> tuple : output) {
	    System.out.println("(" + tuple._1() + "," + tuple._2() + ")");
	}
	System.out.println();
    }
    


    public static void main(String[] args) throws Exception {

	SparkConf conf = new SparkConf().setAppName("CrossCorrelation");
	JavaSparkContext sc = new JavaSparkContext(conf);

	JavaRDD<String> lines = sc.textFile(args[0]);

	JavaPairRDD<String, String> pairs = lines.mapToPair(s -> new Tuple2<>(SPACE.split(s)[0], SPACE.split(s)[1]));
	printPairRDD(pairs);
	
	JavaPairRDD<Tuple2<String, String>, Integer> correlation = pairs.mapToPair(s -> new Tuple2(s, 1));
	printTripletRDD(correlation);
	
	correlation = correlation.reduceByKey((x, y) -> x + y);
	printTripletRDD(correlation);

	sc.stop();
	sc.close();
    }
}
