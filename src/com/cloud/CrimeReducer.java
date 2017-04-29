package com.cloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class CrimeReducer extends Reducer<Text,Text,Text,Text>{
	

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
    	//this is reducer class
    }
}
