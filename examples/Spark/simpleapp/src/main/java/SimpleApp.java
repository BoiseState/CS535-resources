import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("SimpleApp").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    String logFile = "/home/amit/spark-install/spark/README.md"; //Update to your file path 
    JavaRDD<String> logData = sc.textFile(logFile);

    long numAs = logData.filter(s -> s.contains("a")).count();
    long numBs = logData.filter(s -> s.contains("b")).count();
    
    System.out.println();
    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    System.out.println();

    
    JavaRDD<Integer> lineLengths = logData.map(s -> s.length());
    int totalLength = lineLengths.reduce((a, b) -> a + b);
    
    System.out.println();
    System.out.println("Total linelengths = " + totalLength);
    System.out.println();


    sc.stop(); // stop the Spark context
    sc.close();
  }
}
