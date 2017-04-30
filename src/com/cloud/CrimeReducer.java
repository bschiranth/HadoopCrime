package com.cloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;


public class CrimeReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
	
	
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                        throws IOException, InterruptedException {
    	long sum = 0;
    	for(LongWritable val : values)
    	{
    		sum += val.get();
    		
    	}
    	context.write(new Text(key), new LongWritable(sum));
    }
}
