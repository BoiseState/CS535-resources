

import java.io.*; 
import java.util.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 
  
public class TopMoviesMapper extends Mapper<Object, 
                            Text, Text, LongWritable> { 
  
    private TreeMap<Long, String> tmap; 
  
    @Override
    public void setup(Context context) throws IOException, 
                                     InterruptedException 
    { 
        tmap = new TreeMap<Long, String>(); 
    } 
  
    @Override
    public void map(Object key, Text value, 
       Context context) throws IOException,  
                      InterruptedException 
    { 
  
        // input data format => movie_name     
        // no_of_views  (tab seperated) 
        // we split the input data 
        String[] tokens = value.toString().split("\t"); 
  
        String movie_name = tokens[0]; 
        long no_of_views = Long.parseLong(tokens[1]); 
  
        // insert data into treeMap, 
        // we want top 10  viewed movies 
        // so we pass no_of_views as key 
        tmap.put(no_of_views, movie_name); 
  
        // we remove the first key-value 
        // if it's size increases 10 
        if (tmap.size() > 10) 
        { 
            tmap.remove(tmap.firstKey()); 
        } 
    } 
  
    @Override
    public void cleanup(Context context) throws IOException, 
                                       InterruptedException 
    { 
        for (Map.Entry<Long, String> entry : tmap.entrySet())  
        { 
  
            long count = entry.getKey(); 
            String name = entry.getValue(); 
  
            context.write(new Text(name), new LongWritable(count)); 
        } 
    } 
} 