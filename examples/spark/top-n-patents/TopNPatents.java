
/**
 * Find the top N cited patents
 * Format of input:
 * 	citing_patent cited_patent
 * 	citing_patent cited_patent
 * 	citing_patent cited_patent
 *      . . .
 *      
 * @author amit
 */

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.regex.Pattern;

public final class TopNPatents {
	private static final Pattern SPACE = Pattern.compile("\\s+"); // one or more spaces

	public static void printRDD(JavaPairRDD<Integer, Integer> rdd) {
		System.out.println();
		rdd.collect().forEach(x -> System.out.println(x));
		System.out.println();
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: java TopNPatents <file or folder> <N>");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("TopNPatents");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile(args[0]);
		int n = Integer.parseInt(args[1]);

		// create one tuple for each citation of the form (patent#, 1)
		// we are grabbing split(s)[1], which is the second field of the line
		JavaPairRDD<Integer, Integer> patents = lines
				.mapToPair(s -> new Tuple2<>(Integer.parseInt(SPACE.split(s)[1]), 1));
		printRDD(patents);

		// count the number of references to each patent that was cited
		patents = patents.reduceByKey((i, j) -> i + j);
		printRDD(patents);

		// Swap each tuple so we can sort by number of citations
		JavaPairRDD<Integer, Integer> swappedPatents = patents.mapToPair(s -> s.swap());
		printRDD(swappedPatents);

		// Sort in descending order by number of citations
		swappedPatents = swappedPatents.sortByKey(false); // false for descending
		printRDD(swappedPatents);

		// Take the top N and print them
		System.out.println();
		swappedPatents.take(n).forEach(s -> System.out.println(s));
		System.out.println();

		sc.stop();
		sc.close();
	}
}
