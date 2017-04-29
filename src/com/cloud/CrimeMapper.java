package com.cloud;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.FileNotFoundException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import java.io.PrintWriter;

public class CrimeMapper extends Mapper<Text,Text,Text,Text>{
	
	@Override
    protected void map(Text key, Text value, Context context)
                    throws IOException, InterruptedException, FileNotFoundException {
		String[] keys = key.toString().split(",");
		String[] dates = keys[0].split("/");
		
		File newFile = new File("D:/Cloud/input/data/test.csv");
		PrintWriter pw = new PrintWriter(newFile);
		StringBuilder sb = new StringBuilder();
		if(newFile.exists() && !newFile.isDirectory())
		{
			if(dates.length > 1)
			{
				System.out.println("Month is " + dates[0]);
				String[] year = dates[2].split(" ");
				System.out.println(year[0]);
				System.out.println("key is "+keys[1]);
				sb = new StringBuilder();
				sb.append(dates[0]);
				sb.append(',');
				sb.append(year[0]);
				sb.append(',');
				sb.append(keys[1]);
				sb.append(',');
				sb.append(keys[4]);
				sb.append(',');
				sb.append(keys[5]);
				sb.append('\n');
			}
			
		}
		else
		{		
			System.out.println("Here in else");
			sb.append("month");
			sb.append(',');
			sb.append("year");
			sb.append(',');
			sb.append("crimetype");
			sb.append(',');
			sb.append("district");
			sb.append(',');
			sb.append("resolution");
			sb.append('\n');
			 
			
		}
		pw.write(sb.toString());
	    pw.close();
		
	}
	
}
