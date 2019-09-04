import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;

/**
 * MergeResults.java
 * 
 * Merges the intermediate output of the UniqueListeners and SumTrackStats jobs
 * into the final result: Number of UniqueListeners Number of times the track
 * was scrobbled Number of times the track was listened to on the radio Number
 * of times the track was listened to in total Number of times the track was
 * skipped
 * 
 * @author marissa
 * @author amit
 * @see Hadoop: The Definitive Guide - Chapter 16
 * 
 */

public class MergeResults extends Configured implements Tool 
{

	public static class MergeListenersMapper extends MapReduceBase implements
			Mapper<IntWritable, IntWritable, IntWritable, TrackStats> {

		@Override
		public void map(IntWritable trackId, IntWritable uniqueListenerCount,
				OutputCollector<IntWritable, TrackStats> output,
				Reporter reporter) throws IOException {
			TrackStats trackStats = new TrackStats();
			trackStats.setListeners(uniqueListenerCount.get());
			output.collect(trackId, trackStats);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Path sumInputDir = new Path(args[1] + Path.SEPARATOR_CHAR
				+ "sumInputDir");
		Path listenersInputDir = new Path(args[1] + Path.SEPARATOR_CHAR
				+ "listenersInputDir");
		Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "finalSumOutput");

		JobConf conf = new JobConf(UniqueListeners.class);
		conf.setJobName("MergeResults");

		MultipleInputs.addInputPath(conf, sumInputDir,
				SequenceFileInputFormat.class, IdentityMapper.class);
		MultipleInputs.addInputPath(conf, listenersInputDir,
				SequenceFileInputFormat.class, MergeListenersMapper.class);

		conf.setReducerClass(SumTrackStats.SumTrackStatsReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(TrackStats.class);

		FileOutputFormat.setOutputPath(conf, output);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}
}
