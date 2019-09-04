import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Builds an inverted index: each word followed by files it was found in.
 * 
 * 
 */
public class InvertedIndex
{

	public static class InvertedIndexMapper extends
			Mapper<LongWritable, Text, Text, Text>
	{

		private final static Text word = new Text();
		private final static Text location = new Text();

		public void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException
		{

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			location.set(fileName);

			String line = val.toString();
			StringTokenizer itr = new StringTokenizer(line.toLowerCase(),
					" , .;:'\"&!?-_\n\t12345678910[]{}<>\\`~|=^()@#$%^*/+-");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, location);
			}
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, Text, Text, Text>
	{

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{

			boolean first = true;
			Iterator<Text> itr = values.iterator();
			StringBuilder toReturn = new StringBuilder();
			while (itr.hasNext()) {
				if (!first)
					toReturn.append(", ");
				first = false;
				toReturn.append(itr.next().toString());
			}

			context.write(key, new Text(toReturn.toString()));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		if (args.length < 2) {
			System.out
					.println("Usage: InvertedIndex <input path> <output path>");
			System.exit(1);
		}
		Job job = new Job(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
