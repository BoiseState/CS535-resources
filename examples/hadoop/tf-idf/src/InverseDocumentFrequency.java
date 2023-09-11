import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Mapper and Reducer classes to calculate the Inverse Document
 * Frequency of each term. 
 * 
 * @author marissa
 * @author amit
 */
public class InverseDocumentFrequency {
	public static class InverseDocumentFrequencyMapper extends
			Mapper<Text, IntWritable, Text, Text> {

		private Text term = new Text();
		private Text result = new Text();

		/* Input: docId_term termFreq */
		/* Output: <term> <docId_termfreq> */
		
		public void map(Text key, IntWritable value, Context context)
				throws IOException, InterruptedException {

			String[] id_term = key.toString().split("_");

			term.set(id_term[1]);
			result.set(id_term[0] + "_" + value);
			context.write(term, result);
		}
	}

	public static class InverseDocumentFrequencyReducer extends
			Reducer<Text, Text, Text, Text> {

		/* Input: <term> [<docId_termfreq>,...] */
		/* Output: <term> <values.length_docId_termFreq> */
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			/* Use this so we can get the size of the values */
			ArrayList<Text> list = new ArrayList<Text>();
			for (Text t : values)
				list.add(new Text(t));

			/* Prepend the number of documents containing this term. */
			for (Text t : list)
				context.write(key, new Text(list.size() + "_" + t.toString()));
		}
	}
}
