import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GenerateRandomStats extends Configured implements Tool 
{

	static class RangeInputFormat implements
			InputFormat<LongWritable, NullWritable> {

		/**
		 * An input split consisting of a range on numbers.
		 */
		static class RangeInputSplit implements InputSplit {
			long firstRow;
			long rowCount;

			public RangeInputSplit() {
			}

			public RangeInputSplit(long offset, long length) {
				firstRow = offset;
				rowCount = length;
			}

			public long getLength() throws IOException {
				return 0;
			}

			public String[] getLocations() throws IOException {
				return new String[] {};
			}

			public void readFields(DataInput in) throws IOException {
				firstRow = WritableUtils.readVLong(in);
				rowCount = WritableUtils.readVLong(in);
			}

			public void write(DataOutput out) throws IOException {
				WritableUtils.writeVLong(out, firstRow);
				WritableUtils.writeVLong(out, rowCount);
			}
		}

		@Override
		public InputSplit[] getSplits(JobConf job, int numSplits) {
			long totalRows = getNumberOfRows(job);
			long rowsPerSplit = totalRows / numSplits;
			System.out.println("Generating " + totalRows + " using "
					+ numSplits + " maps with step of " + rowsPerSplit);
			InputSplit[] splits = new InputSplit[numSplits];
			long currentRow = 0;
			for (int split = 0; split < numSplits - 1; ++split) {
				splits[split] = new RangeInputSplit(currentRow, rowsPerSplit);
				currentRow += rowsPerSplit;
			}
			splits[numSplits - 1] = new RangeInputSplit(currentRow, totalRows
					- currentRow);
			return splits;
		}

		@Override
		public RecordReader<LongWritable, NullWritable> getRecordReader(
				InputSplit split, JobConf job, Reporter reporter)
				throws IOException {
			return new RangeRecordReader((RangeInputSplit) split);
		}

		static class RangeRecordReader implements
				RecordReader<LongWritable, NullWritable> {
			long startRow;
			long finishedRows;
			long totalRows;

			public RangeRecordReader(RangeInputSplit split) {
				startRow = split.firstRow;
				finishedRows = 0;
				totalRows = split.rowCount;
			}

			public void close() throws IOException {
				// NOTHING
			}

			public LongWritable createKey() {
				return new LongWritable();
			}

			public NullWritable createValue() {
				return NullWritable.get();
			}

			public long getPos() throws IOException {
				return finishedRows;
			}

			public float getProgress() throws IOException {
				return finishedRows / (float) totalRows;
			}

			public boolean next(LongWritable key, NullWritable value) {
				if (finishedRows < totalRows) {
					key.set(startRow + finishedRows);
					finishedRows += 1;
					return true;
				} else {
					return false;
				}
			}

		}

	}

	static long getNumberOfRows(JobConf job) {
		return job.getLong("stats.num-rows", 0);
	}

	static void setNumberOfRows(JobConf job, long numRows) {
		job.setLong("stats.num-rows", numRows);
	}

	public static class GenerateRandomStatsMapper extends MapReduceBase
			implements
			Mapper<LongWritable, NullWritable, TrackStatistics, NullWritable> {

		private TrackStatistics trackStats;
		private Random random;

		@Override
		public void map(LongWritable key, NullWritable value,
				OutputCollector<TrackStatistics, NullWritable> output,
				Reporter reporter) throws IOException {

			random = new Random();
			trackStats = new TrackStatistics(random);

			output.collect(trackStats, value);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			System.out
					.println("Usage: GenerateRandomStats <num> <output path>");
			System.exit(0);
		}
		
		JobConf conf = new JobConf(GenerateRandomStats.class);
		conf.setJobName("GenerateandomStats");

		setNumberOfRows(conf, Long.parseLong(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setJarByClass(GenerateRandomStats.class);
		conf.setMapperClass(GenerateRandomStatsMapper.class);
		conf.setNumReduceTasks(0);
		conf.setOutputKeyClass(TrackStatistics.class);
		conf.setOutputValueClass(NullWritable.class);
		conf.setInputFormat(RangeInputFormat.class);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new GenerateRandomStats(), args);
		System.exit(exitCode);
	}
}
