import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * UniqueListeners.java
 * 
 * Calculates the total number of unique listeners for each track by counting
 * the first listen by a user and ignoring all other listens by the same user.
 * 
 * @author marissa
 * @author amit
 * @see Hadoop: The Definitive Guide - Chapter 16
 *
 */
public class UniqueListeners extends Configured implements Tool
{

    /**
     * Processes space-delimited raw listening data and emits the user ID associated
     * with each track ID.
     */
    public static class UniqueListenersMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>
    {

	@Override
	public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

	    String[] parts = (line.toString()).split(" ");

	    int scrobbles = Integer.parseInt(parts[TrackStatistics.COL_SCROBBLE]);
	    int radioListens = Integer.parseInt(parts[TrackStatistics.COL_RADIO]);

	    /* Ignore track if marked with zero plays */
	    if (scrobbles <= 0 && radioListens <= 0) return;

	    /* Output user id against track id */
	    IntWritable trackId = new IntWritable(Integer.parseInt(parts[TrackStatistics.COL_TRACKID]));
	    IntWritable userId = new IntWritable(Integer.parseInt(parts[TrackStatistics.COL_USERID]));
	    context.write(trackId, userId);
	}
    }

    /**
     * Receives a list of user IDs per track ID and puts them into a set to remove
     * duplicates. The size of this set (the number of unique listeners) is emitted
     * for each track.
     */
    public static class UniqueListenersReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {

	public void reduce(IntWritable trackId, Iterator<IntWritable> values, Context context)
	        throws IOException, InterruptedException {

	    Set<Integer> userIds = new HashSet<Integer>();

	    /* Add all users to set, duplicates automatically removed */
	    while (values.hasNext()) {
		IntWritable userId = values.next();
		userIds.add(Integer.valueOf(userId.get()));
	    }
	    /* Output trackId -> number of unique listeners per track */
	    context.write(trackId, new IntWritable(userIds.size()));
	}
    }


    public int run(String[] args) throws Exception {

	Configuration conf = new Configuration();

	Path input = new Path(args[0]);
	Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "listenersInputDir");

	/* Define the unique listeners job */
	@SuppressWarnings("deprecation")
	Job job = new Job(conf, "unique-listeners");
	job.setJarByClass(UniqueListeners.class);

	/* Map Configuration */
	job.setMapperClass(UniqueListenersMapper.class);

	/* Reduce Configuration */
	job.setReducerClass(UniqueListenersReducer.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);

	/* I/O Configuration */
	FileInputFormat.setInputPaths(job, input);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, output);

	/* Run the first job */
	if (job.waitForCompletion(true))
	    return 0;
	else
	    return 1;
    }

}
