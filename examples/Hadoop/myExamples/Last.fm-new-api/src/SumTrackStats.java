import java.io.IOException;
import java.util.Iterator;

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
 * SumTrackStats.java
 * 
 * Accumulates total listens, scrobbles, radio listens, and skips for each track
 * by counting the values for all listens by all users.
 * 
 * @author marissa
 * @author amit
 * @see Hadoop: The Definitive Guide - Chapter 16
 */
public class SumTrackStats extends Configured implements Tool
{

    public static class SumTrackStatsMapper extends Mapper<LongWritable, Text, IntWritable, TrackStats>
    {

	public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

	    String[] parts = (line.toString()).split(" ");
	    int trackId = Integer.parseInt(parts[TrackStatistics.COL_TRACKID]);
	    int scrobbles = Integer.parseInt(parts[TrackStatistics.COL_SCROBBLE]);
	    int radio = Integer.parseInt(parts[TrackStatistics.COL_RADIO]);
	    int skip = Integer.parseInt(parts[TrackStatistics.COL_SKIP]);

	    TrackStats trackstat = new TrackStats(0, scrobbles + radio, scrobbles, radio, skip);
	    context.write(new IntWritable(trackId), trackstat);
	}
    }

    public static class SumTrackStatsReducer extends Reducer<IntWritable, TrackStats, IntWritable, TrackStats>
    {

	public void reduce(IntWritable trackId, Iterator<TrackStats> values, Context context)
	        throws IOException, InterruptedException {

	    /* Hold totals for this track */
	    TrackStats sum = new TrackStats();
	    while (values.hasNext()) {
		TrackStats trackStats = (TrackStats) values.next();
		sum.setListeners(sum.getListeners() + trackStats.getListeners());
		sum.setPlays(sum.getPlays() + trackStats.getPlays());
		sum.setSkips(sum.getSkips() + trackStats.getSkips());
		sum.setScrobbles(sum.getScrobbles() + trackStats.getScrobbles());
		sum.setRadioPlays(sum.getRadioPlays() + trackStats.getRadioPlays());
	    }
	    context.write(trackId, sum);
	}

    }


    public int run(String[] args) throws Exception {

	Path input = new Path(args[0]);
	Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "sumInputDir");

	/* Define the inverted index job */
	Configuration conf = new Configuration();

	Job job = new Job(conf, "sum-track-stats");
	job.setJarByClass(SumTrackStats.class);

	/* Map Configuration */
	job.setMapperClass(SumTrackStatsMapper.class);

	/* Reduce Configuration */
	job.setReducerClass(SumTrackStatsReducer.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(TrackStats.class);

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
