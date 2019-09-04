import java.io.IOException;
import java.util.Iterator;

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

	public static class SumTrackStatsMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, TrackStats> {

		@Override
		public void map(LongWritable offset, Text line,
				OutputCollector<IntWritable, TrackStats> output,
				Reporter reporter) throws IOException {

			String[] parts = (line.toString()).split(" ");
			int trackId = Integer.parseInt(parts[TrackStatistics.COL_TRACKID]);
			int scrobbles = Integer.parseInt(parts[TrackStatistics.COL_SCROBBLE]);
			int radio = Integer.parseInt(parts[TrackStatistics.COL_RADIO]);
			int skip = Integer.parseInt(parts[TrackStatistics.COL_SKIP]);

			TrackStats trackstat = new TrackStats(0, scrobbles + radio,
					scrobbles, radio, skip);
			output.collect(new IntWritable(trackId), trackstat);
		}
	}

	public static class SumTrackStatsReducer extends MapReduceBase implements
			Reducer<IntWritable, TrackStats, IntWritable, TrackStats> {

		@Override
		public void reduce(IntWritable trackId, Iterator<TrackStats> values,
				OutputCollector<IntWritable, TrackStats> output,
				Reporter reporter) throws IOException {

			/* Hold totals for this track */
			TrackStats sum = new TrackStats();
			while (values.hasNext()) {
				TrackStats trackStats = (TrackStats) values.next();
				sum.setListeners(sum.getListeners() + trackStats.getListeners());
				sum.setPlays(sum.getPlays() + trackStats.getPlays());
				sum.setSkips(sum.getSkips() + trackStats.getSkips());
				sum.setScrobbles(sum.getScrobbles() + trackStats.getScrobbles());
				sum.setRadioPlays(sum.getRadioPlays()
						+ trackStats.getRadioPlays());
			}
			output.collect(trackId, sum);
		}

	}

	@Override
	public int run(String[] args) throws Exception {

		Path input = new Path(args[0]);
		Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "sumInputDir");

		/* Define the inverted index job */
		JobConf conf = new JobConf(SumTrackStats.class);
		conf.setJobName("SumTrackStats");

		/* Map Configuration */
		conf.setMapperClass(SumTrackStatsMapper.class);

		/* Reduce Configuration */
		conf.setReducerClass(SumTrackStatsReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(TrackStats.class);

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
