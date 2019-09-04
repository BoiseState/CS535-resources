import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * UniqueListeners.java
 * 
 * Calculates the total number of unique listeners for each track
 * by counting the first listen by a user and ignoring all other
 * listens by the same user. 
 * 
 * @author marissa
 * @author amit
 * @see Hadoop: The Definitive Guide - Chapter 16
 *
 */
public class UniqueListeners extends Configured implements Tool 
{

	/**
	 * Processes space-delimited raw listening data and emits the 
	 * user ID associated with each track ID.
	 */
	public static class UniqueListenersMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		@Override
		public void map(LongWritable offset, Text line,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {

			String[] parts = (line.toString()).split(" ");

			int scrobbles = Integer.parseInt(parts[TrackStatistics.COL_SCROBBLE]);
			int radioListens = Integer.parseInt(parts[TrackStatistics.COL_RADIO]);

			/* Ignore track if marked with zero plays */
			if (scrobbles <= 0 && radioListens <= 0)
				return;

			/* Output user id against track id */
			IntWritable trackId = new IntWritable(
					Integer.parseInt(parts[TrackStatistics.COL_TRACKID]));
			IntWritable userId = new IntWritable(
					Integer.parseInt(parts[TrackStatistics.COL_USERID]));
			output.collect(trackId, userId);
		}
	}

	/**
	 * Receives a list of user IDs per track ID and puts them into a
	 * set to remove duplicates. The size of this set (the number of 
	 * unique listeners) is emitted for each track. 
	 */
	public static class UniqueListenersReducer extends MapReduceBase implements
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		@Override
		public void reduce(IntWritable trackId, Iterator<IntWritable> values,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {

			Set<Integer> userIds = new HashSet<Integer>();

			/* Add all users to set, duplicates automatically removed */
			while (values.hasNext()) {
				IntWritable userId = values.next();
				userIds.add(Integer.valueOf(userId.get()));
			}
			/* Output trackId -> number of unique listeners per track */
			output.collect(trackId, new IntWritable(userIds.size()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "listenersInputDir");

		/* Define the unique listeners job */
		JobConf conf = new JobConf(UniqueListeners.class);
		conf.setJobName("UniqueListeners");

		/* Map Configuration */
		conf.setMapperClass(UniqueListenersMapper.class);

		/* Reduce Configuration */
		conf.setReducerClass(UniqueListenersReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		/* I/O Configuration */
		FileInputFormat.setInputPaths(conf, input);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(conf, output);

		/* Run the first job */
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace(); 
			return 1;
		}
		return 0;
	}


}
