import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class KeyValueTextTextInputFormat extends FileInputFormat<Text, Text>{

	
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
		TaskAttemptContext context) throws IOException, InterruptedException {
		context.setStatus(split.toString());
		return new KeyValueTextTextRecordReader((FileSplit) split, context);
	}
}
