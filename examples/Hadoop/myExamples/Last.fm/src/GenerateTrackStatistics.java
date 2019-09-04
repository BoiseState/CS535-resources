import org.apache.hadoop.util.ToolRunner;

public class GenerateTrackStatistics {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out
					.println("Usage: GenerateTrackStatistics <input path> <output path>");
			System.exit(0);
		}

		int exitCode = ToolRunner.run(new UniqueListeners(), args);
		
		if (exitCode == 0)
			exitCode = ToolRunner.run(new SumTrackStats(), args);
		else {
			System.err.println("GenerateTrackStatistics: UniqueListeners map-reduce phase failed!!");
			System.exit(1);
		}
			
		if (exitCode == 0)
			exitCode = ToolRunner.run(new MergeResults(), args);
		else {
			System.err.println("GenerateTrackStatistics: SumTrackStats map-reduce phase failed!!");
			System.exit(1);
		}

		System.exit(exitCode);
	}
}
