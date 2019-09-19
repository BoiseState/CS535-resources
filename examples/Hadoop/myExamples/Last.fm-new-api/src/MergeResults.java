import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

    public static class MergeListenersMapper extends Mapper<IntWritable, IntWritable, IntWritable, TrackStats>
    {

	public void map(IntWritable trackId, IntWritable uniqueListenerCount, Context context)
	        throws IOException, InterruptedException {
	    TrackStats trackStats = new TrackStats();
	    trackStats.setListeners(uniqueListenerCount.get());
	    context.write(trackId, trackStats);
	}
    }


    public int run(String[] args) throws Exception {

	Configuration conf = new Configuration();

	Path sumInputDir = new Path(args[1] + Path.SEPARATOR_CHAR + "sumInputDir");
	Path listenersInputDir = new Path(args[1] + Path.SEPARATOR_CHAR + "listenersInputDir");
	Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "finalSumOutput");

	@SuppressWarnings("deprecation")
	Job job = new Job(conf, "merge-results");
	job.setJarByClass(UniqueListeners.class);

	MultipleInputs.addInputPath(job, sumInputDir, SequenceFileInputFormat.class, Mapper.class);
	MultipleInputs.addInputPath(job, listenersInputDir, SequenceFileInputFormat.class, MergeListenersMapper.class);

	job.setReducerClass(SumTrackStats.SumTrackStatsReducer.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(TrackStats.class);

	FileOutputFormat.setOutputPath(job, output);
	if (job.waitForCompletion(true))
	    return 0;
	else
	    return 1;
    }
}
