import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class KeyValueTextIntRecordReader extends RecordReader<Text, IntWritable> {

	private static LineRecordReader lineRecordReader;
	private Text theKey;
	private IntWritable theValue;

	public KeyValueTextIntRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		lineRecordReader = new LineRecordReader();
		this.initialize(split, context);
	}


	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		lineRecordReader.initialize(split, context);
	}

	
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(lineRecordReader.nextKeyValue() == false)
			return false;
		
		String[] line = lineRecordReader.getCurrentValue().toString().split("\t");
		if (line.length != 2)
			throw new IOException("Invalid record received");
		
		theKey = new Text(line[0]);
		theValue = new IntWritable(Integer.parseInt(line[1]));
			
		return true;
	}

	
	public Text getCurrentKey() throws IOException, InterruptedException {
		return theKey;
	}

	
	public IntWritable getCurrentValue() throws IOException,
			InterruptedException {
		return theValue;
	}

	
	public void close() throws IOException {
		lineRecordReader.close();
	}

	
	public float getProgress() throws IOException {
		return lineRecordReader.getProgress();
	}
}
