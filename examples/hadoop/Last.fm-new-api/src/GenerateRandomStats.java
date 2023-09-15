import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GenerateRandomStats extends Configured implements Tool
{

    static class RangeInputFormat extends InputFormat<LongWritable, NullWritable>
    {

	/**
	 * An input split consisting of a range on numbers.
	 */
	static class RangeInputSplit extends InputSplit
	{
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

	static class RangeRecordReader extends RecordReader<LongWritable, NullWritable>
	{
	    long startRow;
	    long finishedRows;
	    long totalRows;
	    private LongWritable key = new LongWritable();


	    public RangeRecordReader(RangeInputSplit split) {
		startRow = split.firstRow;
		finishedRows = 0;
		totalRows = split.rowCount;
	    }


	    public void close() throws IOException {
		// No effect in this setup
	    }


	    public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	    }


	    public NullWritable getCurrentValue() throws IOException, InterruptedException {
		return NullWritable.get();
	    }


	    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
	    }


	    public boolean nextKeyValue() throws IOException, InterruptedException {
		if (finishedRows < totalRows) {
		    key.set(startRow + finishedRows);
		    finishedRows += 1;
		    return true;
		} else {
		    return false;
		}
	    }


	    public float getProgress() throws IOException, InterruptedException {
		return finishedRows / (float) totalRows;
	    }
	}


	public RecordReader<LongWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext arg1)
	        throws IOException, InterruptedException {
	    return new RangeRecordReader((RangeInputSplit) split);
	}


	public List<InputSplit> getSplits(JobContext context) throws IOException {
	    Configuration conf = context.getConfiguration();
	    long totalRows = conf.getLong("stats.num-rows", 0);
	    int numSplits = conf.getInt("stats.num-splits", 0);
	    long rowsPerSplit = totalRows / numSplits;
	    System.out
	            .println("Generating " + totalRows + " using " + numSplits + " maps with step of " + rowsPerSplit);
	    List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
	    long currentRow = 0;
	    for (int split = 0; split < numSplits - 1; ++split) {
		splits.add(new RangeInputSplit(currentRow, rowsPerSplit));
		currentRow += rowsPerSplit;
	    }
	    splits.add(new RangeInputSplit(currentRow, totalRows - currentRow));
	    return splits;
	}

    }

    public static class GenerateRandomStatsMapper
            extends Mapper<LongWritable, NullWritable, TrackStatistics, NullWritable>
    {

	private TrackStatistics trackStats;
	private Random random;


	@Override
	public void map(LongWritable key, NullWritable value, Context context)
	        throws IOException, InterruptedException {
	    trackStats = new TrackStatistics(random);
	    context.write(trackStats, value);
	}
    }


    @Override
    public int run(String[] args) throws Exception {
	int status = 0;

	Configuration conf = new Configuration();
	if (args.length < 2) {
	    System.out.println("Usage: GenerateRandomStats <num> <output path>");
	    System.exit(0);
	}

	conf.setLong("stats.num-rows", Long.parseLong(args[0]));
	conf.setInt("stats.num-splits", 2);
	Job job = new Job(conf, "generate-random-stats");
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setJarByClass(GenerateRandomStats.class);
	job.setMapperClass(GenerateRandomStatsMapper.class);
	job.setNumReduceTasks(0);
	job.setOutputKeyClass(TrackStatistics.class);
	job.setOutputValueClass(NullWritable.class);
	job.setInputFormatClass(RangeInputFormat.class);
	try {
	    status = (job.waitForCompletion(true) ? 0 : 1);
	} catch (Exception e) {
	    e.printStackTrace();
	    return 1;
	}
	return status;
    }


    public static void main(String[] args) throws Exception {
	int exitCode = ToolRunner.run(new GenerateRandomStats(), args);
	System.exit(exitCode);
    }
}
