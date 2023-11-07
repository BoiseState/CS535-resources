import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDDPlayground
{
    public static void main(String[] args)
    {
	SparkConf conf = new SparkConf().setAppName("SparkExercise");
	JavaSparkContext sc = new JavaSparkContext(conf);

	List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	JavaRDD<Integer> rdd1 = sc.parallelize(data);
	printRDD("--> original RDD", rdd1);

	JavaRDD<Integer> rdd2 = rdd1.map(x -> x + 1);
	printRDD("--> distData1 = distData.map(x -> x + 1);", rdd2);

	JavaRDD rdd3 = rdd1.filter(x -> x % 2 == 0);
	printRDD("--> distData1 = distData.filter(x -> x % 2 == 0);", rdd3);

	JavaRDD<Integer> rdd4 = sc.parallelize(Arrays.asList(1, 2, 2, 3, 3, 3, 4));
	JavaRDD<Integer> rdd5 = rdd4.distinct();
	printRDD("--> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 3); rdd1 = rdd1.distinct();", rdd5);

	List<List<Integer>> x = Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(2, 3), Arrays.asList(3));
	JavaRDD<List<Integer>> rdd6 = sc.parallelize(x);

	System.out.println(
	        "--> rdd = sc.parallelize(Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(2, 3), Arrays.asList(3)));");
	rdd6.collect().forEach(s -> System.out.print(s + " "));
	System.out.println();

	System.out.print("result = rdd6.flatMap(s -> s.iterator()) --> ");
	JavaRDD<Integer> result = rdd6.flatMap(s -> s.iterator());
	result.collect().forEach(s -> System.out.print(s + " "));
	System.out.println();

//	// range in Java -- however it won't work because of how it converts
//	List<Integer> set1 = IntStream.rangeClosed(1, 3).boxed().collect(Collectors.toList());
//	List<Integer> set2 = IntStream.rangeClosed(2, 3).boxed().collect(Collectors.toList());
//	List<Object> y = Arrays.asList(set1, set2, 3);
//	//System.out.println(y);
	
	// Word count in Spark
	JavaRDD<String> lines = sc.textFile("../word-count/input/Alice-in-Wonderland.txt");
	
	Pattern SPACE = Pattern.compile(" ");
	JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
	JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
	JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
	
	System.out.println("Word count top 10: ");
	List<Tuple2<String, Integer>> output = counts.takeOrdered(10, null);
	for (Tuple2<?, ?> tuple : output) {
	    System.out.println(tuple._1() + ": " + tuple._2());
	}
	
	
	JavaRDD<Integer> rdd7 = sc.parallelize(Arrays.asList(1, 2, 3, 2, 3, 4, 5, 6, 6));
	JavaRDD<Integer> sample = rdd7.sample(false, 0.5);
	printRDD("rdd7.sample(false, 0.5)", sample);

	JavaRDD<Integer> set1 = sc.parallelize(Arrays.asList(1, 2, 3));
	JavaRDD<Integer> set2 = sc.parallelize(Arrays.asList(3, 4, 5));
	JavaRDD<Integer> union = set1.union(set2);
	printRDD("set1 = (1, 2, 3) set2 = (3, 4, 5) set1.union(set2)", union);

	JavaRDD<Integer> intersection = set1.intersection(set2);
	printRDD("set1 = (1, 2, 3) set2 = (3, 4, 5) set1.intersection(set2)", intersection);

	JavaRDD<Integer> subtract = set1.subtract(set2);
	printRDD("set1 = (1, 2, 3) set2 = (3, 4, 5) set1.subtract(set2)", subtract);

	JavaPairRDD<Integer, Integer> cartesian = rdd1.cartesian(rdd2);
	printPairRDD("rdd1 = (1, 2, 3) rdd2 = (3, 4, 5) rdd1.cartesian(rdd2)", cartesian);

	JavaPairRDD<Integer, Iterable<Integer>> set3 = cartesian.groupByKey();
	System.out.println("cartesian.groupByKey()");
	set3.collect().forEach(z -> System.out.print(z + " "));
	System.out.println();
	System.out.println();

	JavaPairRDD<Integer, Integer> set4 = cartesian.reduceByKey((a, b) -> a + b);
	printPairRDD("set4 = cartesian.reduceByKey((a,b) -> a + b);", set4);

	JavaPairRDD<Object, Object> set5 = set4.mapToPair(s -> new Tuple2<>(s, 1));
	set5 = set5.mapValues(r -> r + 1);
	printPairRDD("set5.mapValues(r -> r + 1);", set5);

	JavaPairRDD<Integer, List<Integer>> set6 = set3.mapToPair(s -> new Tuple2<>(s, Arrays.asList(1, 2, 3)));
	rdd6.collect().forEach(s -> System.out.print(s + " "));
	System.out.println();

	JavaPairRDD<Integer, Integer> set7 = set6.flatMapValues(s -> s.iterator());
	printPairRDD("test", set7);
	sc.stop();
	sc.close();
    }


    private static void printRDD(String message, JavaRDD<?> rdd)
    {
	System.out.println(message);
	rdd.collect().forEach(x -> System.out.print(x + " "));
	System.out.println();
	System.out.println();
    }


    private static void printPairRDD(String message, JavaPairRDD<Integer, Integer> rdd)
    {
	System.out.println(message);
	rdd.collect().forEach(x -> System.out.print(x + " "));
	System.out.println();
	System.out.println();
    }

}
