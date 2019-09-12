

import java.io.*; 
import java.util.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 
  
public class TopMoviesMapper extends Mapper<Object, 
                            Text, Text, LongWritable> { 
  
    private TreeMap<Long, List<String>> tmap; 
  
    @Override
    public void setup(Context context) throws IOException, 
                                     InterruptedException 
    { 
        tmap = new TreeMap<Long, List<String>>(); 
    } 
  
    @Override
    public void map(Object key, Text value, 
       Context context) throws IOException,  
                      InterruptedException 
    { 
  
        // input data format => movie_name     
        // no_of_views  (tab separated) 
        // we split the input data 
        String[] tokens = value.toString().split("\t"); 
  
        String movie_name = tokens[0]; 
        long no_of_views = Long.parseLong(tokens[1]); 
  
        // insert data into treeMap, 
        // we want top 10  viewed movies 
        // so we pass no_of_views as key
        List<String> list = tmap.get(no_of_views);
        if (list == null) {
            list = new ArrayList<String>();
        } 
        list.add(movie_name);
        tmap.put(no_of_views, list); 
  
        // we remove the first key-value 
        // if it's size increases beyond 10 
        if (tmap.size() > 10) 
        { 
            tmap.remove(tmap.firstKey()); 
        } 
    } 
  
    @Override
    public void cleanup(Context context) throws IOException, 
                                       InterruptedException 
    { 
        for (Map.Entry<Long, List<String>> entry : tmap.entrySet())  
        { 
            long count = entry.getKey(); 
            List<String> name = entry.getValue(); 
  
	    for (String s : name) {
		context.write(new Text(s), new LongWritable(count));
	    }
        } 
    } 
} 