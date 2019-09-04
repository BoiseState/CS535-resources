import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Calculate the Term Frequency for each term in each document. 
 * 
 * @author marissa
 * @author amit
 */
public class TermFrequency {
	
	protected static boolean caseSensitive = false;
	protected static String delimeters = " , .;:'\"&!?-_\n\t12345678910[]{}<>\\`~|=^()@#$%^*/+-";

	public static class TermFrequencyMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private Set<String> documentIds = new HashSet<String>();

		private final static IntWritable one = new IntWritable(1);
		private Text term = new Text();

		/* Input: <term> <term> ... <term> */
		/* Output: <docId_term> <1> */
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
			
			StringTokenizer tokenizer = new StringTokenizer(line, delimeters);

			/* Map each term and documentID to a value of 1 */
			while (tokenizer.hasMoreTokens()) {
				term.set(getDocumentId(context) + "_" + tokenizer.nextToken());
				context.write(term, one);
				
				/* Keep track of the total number of tokens */
				context.getCounter(TfIdfDriver.Counters.TERMS).increment(1);
			}
		}

		/**
		 * Returns the HashCode of the current document name for the given
		 * Context.
		 * 
		 * @param context
		 *            The current context used by the mapper.
		 * @return The HashCode of the document name.
		 */
		public String getDocumentId(Context context) {
			String hashCode = ((FileSplit) context.getInputSplit()).getPath().getName();
			
			/* If we haven't seen this document ID yet, increment the number of
			 * documents. */
			if (!(documentIds.contains(hashCode))) {
				documentIds.add(hashCode);
				context.getCounter(TfIdfDriver.Counters.DOCUMENTS).increment(1);
			}
			return hashCode;
		}
	}

	public static class TermFrequencyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		/* Input: <docId_term> [<1>,...,<1>] */
		/* Output: <docId_term> <termFreq> */
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			/* Sum all the values to get the total for the term */
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();

			context.write(key, new IntWritable(sum));
		}
	}
}
