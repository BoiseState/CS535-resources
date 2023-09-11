
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map; 
import java.util.TreeMap; 
  
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer; 
  
public class TopMoviesReducer extends Reducer<Text, 
                     LongWritable, LongWritable, Text> { 
  
    private TreeMap<Long, List<String>> tmap; 
  
    @Override
    public void setup(Context context) throws IOException, 
                                     InterruptedException 
    { 
        tmap = new TreeMap<Long, List<String>>(); 
    } 
  
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, 
      Context context) throws IOException, InterruptedException 
    { 
  
        // input data from mapper 
        // key                values 
        // movie_name         [ count ] 
        String name = key.toString(); 
        long count = 0; 
  
        for (LongWritable val : values) 
        { 
            count = val.get(); 
        } 
  
        // insert data into treeMap, 
        // we want top 10 viewed movies 
        // so we pass count as key 
        List<String> list = tmap.get(count);
        if (list == null) {
            list = new ArrayList<String>();
        } 
        list.add(name);
        tmap.put(count, list);
  
        // We remove the first key-value 
        // if the map size increases beyond 10 
        if (tmap.size() > 10) 
        { 
            tmap.remove(tmap.firstKey()); 
        } 
    } 
  
    @Override
    public void cleanup(Context context) throws IOException, 
                                       InterruptedException 
    { 
		for (Map.Entry<Long, List<String>> entry: tmap.entrySet()) {
			long count = entry.getKey();
			List<String> name = entry.getValue();
			for (String s: name) {
				context.write(new LongWritable(count), new Text(s));
			}
		}
    } 
} 