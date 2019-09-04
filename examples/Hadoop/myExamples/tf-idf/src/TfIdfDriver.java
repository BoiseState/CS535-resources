import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * The driver that ties the three map-reduce classes together to do the Tf-IDF
 * calculation.
 * 
 * @author marissa
 * @author amit
 *
 */
public class TfIdfDriver
{

	static enum Counters {
		DOCUMENTS, TERMS
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <input> <output>");
			System.exit(2);
		}

		String input = otherArgs[0];
		String tfOutput = otherArgs[1] + Path.SEPARATOR_CHAR + "tfOutput";
		String idfOutput = otherArgs[1] + Path.SEPARATOR_CHAR + "idfOutput";
		String output = otherArgs[1] + Path.SEPARATOR_CHAR + "finalOutput";

		Job jobTF = new Job(conf, "TF");
		jobTF.setJarByClass(TermFrequency.class);
		jobTF.setMapperClass(TermFrequency.TermFrequencyMapper.class);
		jobTF.setCombinerClass(TermFrequency.TermFrequencyReducer.class);
		jobTF.setReducerClass(TermFrequency.TermFrequencyReducer.class);
		jobTF.setOutputFormatClass(TextOutputFormat.class);
		jobTF.setOutputKeyClass(Text.class);
		jobTF.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(jobTF, new Path(input));
		FileOutputFormat.setOutputPath(jobTF, new Path(tfOutput));

		Job jobIDF = new Job(conf, "IDF");
		jobIDF.setJarByClass(InverseDocumentFrequency.class);
		jobIDF.setMapperClass(InverseDocumentFrequency.InverseDocumentFrequencyMapper.class);
		jobIDF.setReducerClass(InverseDocumentFrequency.InverseDocumentFrequencyReducer.class);
		jobIDF.setInputFormatClass(KeyValueTextIntInputFormat.class);
		jobIDF.setOutputKeyClass(Text.class);
		jobIDF.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIDF, new Path(tfOutput));
		FileOutputFormat.setOutputPath(jobIDF, new Path(idfOutput));

		Job jobTfIdf = new Job(conf, "TF-IDF");
		jobTfIdf.setJarByClass(TfIdf.class);
		jobTfIdf.setMapperClass(TfIdf.TfIdfMapper.class);
		jobTfIdf.setCombinerClass(TfIdf.TfIdfReducer.class);
		jobTfIdf.setReducerClass(TfIdf.TfIdfReducer.class);
		jobTfIdf.setInputFormatClass(KeyValueTextTextInputFormat.class);
		jobTfIdf.setOutputKeyClass(Text.class);
		jobTfIdf.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(jobTfIdf, new Path(idfOutput));
		FileOutputFormat.setOutputPath(jobTfIdf, new Path(output));

		if (jobTF.waitForCompletion(true)) {
			long totalTermCount = jobTF.getCounters()
					.findCounter(Counters.TERMS).getValue();
			long totalDocCount = jobTF.getCounters()
					.findCounter(Counters.DOCUMENTS).getValue();

			if (jobIDF.waitForCompletion(true)) {
				jobTfIdf.getConfiguration()
						.setLong("termCount", totalTermCount);
				jobTfIdf.getConfiguration().setLong("docCount", totalDocCount);
				System.exit(jobTfIdf.waitForCompletion(true) ? 0 : 1);
			}
		}
		System.exit(1);
	}
}
