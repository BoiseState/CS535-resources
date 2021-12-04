
/**
 *  
 *  Calculate cross correlation of a list of items. 
 *  Given a set of 2-tuples of items, for each possible unique pair of items 
 *  calculate the number of tuples where these items co-occur. 
 *  The input consists of text files with one order per line.
 *
 *  @author amit 
 *  
 */

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class CrossCorrelation
{
    private static final Pattern SPACE = Pattern.compile("\\s+");

    public static void printRDD(JavaRDD<?> rdd) {
        rdd.collect().forEach(x -> System.out.println(x + " "));
        System.out.println();
    }

    public static void printPairRDD(JavaPairRDD<?, ?> rdd) {
        rdd.collect().forEach(x -> System.out.println(x + " "));
        System.out.println();
    }


    public static void printTripletRDD(JavaPairRDD<Tuple2<String, String>, Integer> rdd) {
        rdd.collect().forEach(x -> System.out.println(x + " "));
        System.out.println();
    }


    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("CrossCorrelation");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);
        printRDD(lines);

        /* Generate all pairs from each line (as they are in the same order).
         * Each pair has item listed in alphabetical order, so (shoes, bags) is the same as (bags, shoes)
         * We don't add (shoes, shoes) to the list.
         */
        JavaRDD<Tuple2<String, String>> pairs = lines.flatMap( s -> {
            String[] x = SPACE.split(s);
            ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
            // Generate all unique pairs from items on one line (each pair is listed sorted)
            for (int i = 0; i < x.length; i++)
                for (int j = i; j < x.length; j++)
                    if (x[i].compareTo(x[j]) < 0) list.add(new Tuple2<String, String>(x[i], x[j])); 
                    else if (x[i].compareTo(x[j]) > 0) list.add(new Tuple2<String, String>(x[j], x[i]));
            /* else  --> they are equal so we don't add to the list */

            return list.iterator();
        }); 
        printRDD(pairs);
        
        //purse shoes toothbrush lipstick soap -> cartesian, sort, filter

        JavaPairRDD<Tuple2<String, String>, Integer> correlation = 
                pairs.mapToPair(s -> new Tuple2<Tuple2<String, String>, Integer>(s, 1));
        printTripletRDD(correlation);

        correlation = correlation.reduceByKey((x, y) -> x + y);
        printTripletRDD(correlation);

        JavaPairRDD<Integer, Tuple2<String, String>> swapped = correlation.mapToPair(s -> s.swap());
        swapped = swapped.sortByKey(false);

        List<Tuple2<Integer, Tuple2<String, String>>> output = swapped.collect();
        System.out.println();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println("(" + tuple._2() + "," + tuple._1() + ")");
        }
        System.out.println();

        sc.stop();
        sc.close();
    }
}
