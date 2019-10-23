import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SimpleApp2 {
  public static void main(String[] args) {
      
    Logger log = LogManager.getRootLogger();
    log.setLevel(Level.WARN);
    
    SparkConf conf = new SparkConf().setAppName("SimpleApp");
    JavaSparkContext sc = new JavaSparkContext(conf);

    String logFile = "/home/amit/spark-install/spark/README.md"; //Update to your file path 
    JavaRDD<String> logData = sc.textFile(logFile);
    
    System.out.println("#partitions " + logData.getNumPartitions());
    System.out.println("#storage level " + logData.getStorageLevel());


    long numAs = logData.filter(s -> s.contains("a")).count();
    long numBs = logData.filter(s -> s.contains("b")).count();
    
    System.out.println();
    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    System.out.println();

    
    JavaRDD<Integer> lineLengths = logData.map(s -> s.length());
    System.out.println("#partitions " + lineLengths.getNumPartitions());
    System.out.println("#storage level " + lineLengths.getStorageLevel());

    int totalLength = lineLengths.reduce((a, b) -> a + b);
    
    System.out.println();
    System.out.println("Total linelengths = " + totalLength);
    System.out.println();


    sc.stop(); // stop the Spark context
    sc.close();
  }
}
