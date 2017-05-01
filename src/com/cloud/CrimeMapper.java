package com.cloud;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.FileNotFoundException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.PrintWriter;

public class CrimeMapper extends Mapper<Text,Text,Text,Text>{
	
	String [] allMonths = {"Dummy","Jan","Feb","Mar","Apr","May","June","July","Aug","Sept","Oct","Nov","Dec"};
	
	@Override
    protected void map(Text key, Text value, Context context)
                    throws IOException, InterruptedException, FileNotFoundException {
		//System.out.println(key);
		String[] keys = key.toString().split(",");
		String[] dates = keys[0].split("/");
		long mapperValue = 0;
		//File newFile = new File("D:/Cloud/input/data/test.csv");
		//PrintWriter pw = new PrintWriter(newFile);
		StringBuilder mapperKey = new StringBuilder();
		//if(newFile.exists() && !newFile.isDirectory())
		//{
			if(dates.length > 1)
			{
				//System.out.println("Month is " + dates[0]);
				//Month
				//mapperKey.append(dates[0]);
				//mapperKey.append(' ');
				mapperKey.append(allMonths[Integer.parseInt(dates[0])]).append(' ');
				
				String[] year = dates[2].split(" ");
				//System.out.println(year[0]);
				mapperKey.append(year[0]);
				mapperKey.append(' ');
				mapperKey.append(keys[4]);
				//mapperKey.append(' ');
				//mapperKey.append(keys[1]);
				System.out.println(keys[5]);
				if(keys[5].equals("NONE"))
					mapperValue = 0;
				else
					mapperValue = 1;
				context.write(new Text(mapperKey.toString()), new Text(keys[1].toString()));
			}
			
		//}
		/*else
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
	    pw.close();*/
		
	}
	
}
