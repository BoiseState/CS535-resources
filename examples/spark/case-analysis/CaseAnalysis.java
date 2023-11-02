
/** 
 * Calculate the percentage capitalization for each letter A-Z in a collection of text files.
 * @author: amit 
 **/

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public final class CaseAnalysis
{
    public static void main(String[] args) throws Exception
    {
	if (args.length < 1) {
	    System.err.println(
	            "Usage: spark-submit --class \"CaseAnalysis\" --master local[*]" + " case-analysis.jar input");
	    System.exit(1);
	}

	SparkConf conf = new SparkConf().setAppName("CaseAnalysis");
	JavaSparkContext sc = new JavaSparkContext(conf);

	JavaRDD<String> lines = sc.textFile(args[0]);
	JavaRDD<String> letters = lines.flatMap(s -> Arrays.asList(s.split("")).iterator());
	long n = letters.count();
	System.out.println("total #characters = " + n);

	JavaPairRDD<String, Long> ones = letters.mapToPair(s -> {
	    Tuple2<String, Long> t = new Tuple2<String, Long>("other", 0L);
	    if (s.length() > 0) {
		if (Character.isUpperCase(s.charAt(0)))
		    t = new Tuple2<>(s.toUpperCase(), 1L);
		else if (Character.isLowerCase(s.charAt(0))) t = new Tuple2<>(s.toUpperCase(), 0L);
	    }
	    return t;
	});

	// This provides us with the number of times each character is capitalized
	JavaPairRDD<String, Long> countUppers = ones.reduceByKey((i1, i2) -> i1 + i2);
	countUppers.saveAsTextFile("output1");
	printRDD(countUppers);

	JavaPairRDD<String, Long> all = letters.mapToPair(s -> {
	    Tuple2<String, Long> t = new Tuple2<String, Long>("other", 1L);
	    if (s.length() > 0) {
		if (Character.isUpperCase(s.charAt(0)))
		    t = new Tuple2<>(s.toUpperCase(), 1L);
		else if (Character.isLowerCase(s.charAt(0))) t = new Tuple2<>(s.toUpperCase(), 1L);
	    }
	    return t;
	});

	// This provides us with the total counts for each character
	JavaPairRDD<String, Long> countAll = all.reduceByKey((i1, i2) -> i1 + i2);
	countAll.saveAsTextFile("output2");
	printRDD(countAll);

	// Join the two RDDs
	JavaPairRDD<String, Tuple2<Long, Long>> total = countUppers.join(countAll);
	printRDD2(total);

	// Calculate the percentage capitalization
	JavaPairRDD<String, Double> x = total.mapToPair(s -> {
	    return new Tuple2<>(s._1, 100 * (double) s._2._1 / s._2._2);
	});
	printRDD3(x);

	sc.stop();
	sc.close();
    }


    public static void printRDD(JavaPairRDD<String, Long> rdd)
    {
	List<Tuple2<String, Long>> output = rdd.collect();
	System.out.println();
	for (Tuple2<?, ?> tuple : output) {
	    System.out.println("(" + tuple._1() + "," + tuple._2() + ")");
	}
	System.out.println();
    }

    public static void printRDD2(JavaPairRDD<String, Tuple2<Long, Long>> rdd)
    {
	List<Tuple2<String, Tuple2<Long, Long>>> output = rdd.collect();
	System.out.println();
	for (Tuple2<?, ?> tuple : output) {
	    System.out.println("(" + tuple._1() + "," + tuple._2() + ")");
	}
	System.out.println();
    }
    
    public static void printRDD3(JavaPairRDD<String, Double> rdd)
    {
	List<Tuple2<String, Double>> output = rdd.collect();
	System.out.println();
	for (Tuple2<?, ?> tuple : output) {
	    System.out.printf("( %s, %4.2f )\n", tuple._1(), tuple._2());
	}
	System.out.println();
    }

}
