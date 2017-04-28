package com.cloud;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class CrimeMapper extends Mapper<Text,Text,Text,Text>{
	
	@Override
    protected void map(Text key, Text value, Context context)
                    throws IOException, InterruptedException {
		
	}
	
}
