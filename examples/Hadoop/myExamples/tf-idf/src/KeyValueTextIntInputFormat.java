import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class KeyValueTextIntInputFormat extends FileInputFormat<Text, IntWritable>{


	public RecordReader<Text, IntWritable> createRecordReader(InputSplit split,
		TaskAttemptContext context) throws IOException, InterruptedException {
		context.setStatus(split.toString());
		return new KeyValueTextIntRecordReader((FileSplit) split, context);
	}
}
