import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Mapper and Reducer classes to calculate the final value of
 * TF-IDF for each term.
 * 
 * @author marissa
 * @author amit
 */
public class TfIdf {


	public static class TfIdfMapper extends
			Mapper<Text, Text, Text, DoubleWritable> {
		
		/* Input: <term> <docCount_docId_countInDoc> */
		/* Output: <docId_term> <idf> */
		
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] docCount_docId_countInDoc = value.toString().split("_");
			
			long totalTermCount = context.getConfiguration().getLong("termCount", 0);
			long totalDocCount = context.getConfiguration().getLong("docCount", 0);

			double tf = ((double) Integer.parseInt(docCount_docId_countInDoc[2]) / totalTermCount);
			double idf = Math.log((double) totalDocCount / (double) Integer.parseInt(docCount_docId_countInDoc[0]));
			double tfidf = tf * idf;

			context.write(new Text(docCount_docId_countInDoc[1] + "_" + key), new DoubleWritable(tfidf));
		}
	}

	public static class TfIdfReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		/* Input: <docId_term> [<1>,...,<1>] */
		/* Output: <docId_term> <termFreq> */
		
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			for (DoubleWritable val : values)
				context.write(key, val);
		}
	}
}
