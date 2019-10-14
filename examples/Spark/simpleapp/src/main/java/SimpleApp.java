import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;

public class SimpleApp {
  public static void main(String[] args) {
    String logFile = "/home/amit/spark-install/spark/README.md"; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("SimpleApp").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    //Dataset<String> logData = spark.read().textFile(logFile).cache();
    JavaRDD<String> logData = sc.textFile(logFile);


    long numAs = logData.filter(s -> s.contains("a")).count();
    long numBs = logData.filter(s -> s.contains("b")).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
  }
}
