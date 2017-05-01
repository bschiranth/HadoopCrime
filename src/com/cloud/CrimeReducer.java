package com.cloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.File;
import java.util.StringTokenizer;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class CrimeReducer extends Reducer<Text,Text,Text,Text>{
	Map<String, Integer> crimeMap;
	StringBuilder sb;
	File newFile;
	PrintWriter pw;
	@Override
	protected void setup(Context context) throws FileNotFoundException
	{
		crimeMap = new HashMap<String, Integer>();
		crimeMap.put("WARRANTS",0);
		crimeMap.put("OTHER OFFENSES",0);
		crimeMap.put("LARCENY/THEFT",0);
		crimeMap.put("VEHICLE THEFT",0);
		crimeMap.put("VANDALISM",0);
		crimeMap.put("NON-CRIMINAL",0);
		crimeMap.put("ROBBERY",0);
		crimeMap.put("ASSAULT",0);
		crimeMap.put("WEAPON LAWS",0);
		crimeMap.put("BURGLARY",0);
		crimeMap.put("SUSPICIOUS OCC",0);
		crimeMap.put("DRUNKENNESS",0);
		crimeMap.put("FORGERY/COUNTERFEITING",0);
		crimeMap.put("DRUG/NARCOTIC",0);
		crimeMap.put("STOLEN PROPERTY",0);
		crimeMap.put("SECONDARY CODES",0);
		crimeMap.put("TRESPASS",0);
		crimeMap.put("MISSING PERSON",0);
		crimeMap.put("FRAUD",0);
		crimeMap.put("KIDNAPPING",0);
		crimeMap.put("RUNAWAY",0);
		crimeMap.put("DRIVING UNDER THE INFLUENCE",0);
		crimeMap.put("SEX OFFENSES FORCIBLE",0);
		crimeMap.put("PROSTITUTION",0);
		crimeMap.put("DISORDERLY CONDUCT",0);
		crimeMap.put("ARSON",0);
		crimeMap.put("FAMILY OFFENSES",0);
		crimeMap.put("LIQUOR LAWS",0);
		crimeMap.put("BRIBERY",0);
		crimeMap.put("EMBEZZLEMENT",0);
		crimeMap.put("SUICIDE",0);
		crimeMap.put("LOITERING",0);
		crimeMap.put("SEX OFFENSES NON FORCIBLE",0);
		crimeMap.put("EXTORTION",0);
		crimeMap.put("GAMBLING",0);
		crimeMap.put("BAD CHECKS",0);
		crimeMap.put("TREA",0);
		crimeMap.put("RECOVERED VEHICLE",0);
		crimeMap.put("PORNOGRAPHY/OBSCENE MAT",0);
		newFile = new File("D:/Cloud/input/data/test.csv");
		sb = new StringBuilder();
		pw = new PrintWriter(newFile);
	System.out.println(crimeMap);
	}
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException, FileNotFoundException {
    	//splitting keys
    	String[] allKeys = key.toString().split(" ");
    	for(String str:allKeys) pw.append(str+",");
    	
    	
		//PrintWriter pw = new PrintWriter(newFile);
    	//long sum = 0;
    	for(Text val : values)
    	{
    		//sum += val.get();
    		System.out.println(val);
    		crimeMap.put(val.toString(), crimeMap.get(val.toString()) + 1);
    		//context.write(new Text(key), val);
    	}
    	/*if(newFile.exists() && !newFile.isDirectory())
		{
    		sb.append(key.toString());
    		sb.append(",");
		}
    	*/
    	for(Map.Entry<String, Integer> entry : crimeMap.entrySet()) {
    		if(newFile.exists() && !newFile.isDirectory())
    		{
    			//sb.append(entry.getKey());
    			//sb.append("-->");
    			sb.append(entry.getValue());
    			sb.append(",");
    		}
    	}
    	sb.append("\n");
    	pw.append(sb.toString());
	    context.write(key, new Text(sb.toString()));
    	sb.setLength(0);
    	for (Map.Entry<String, Integer> entry : crimeMap.entrySet()) {
            crimeMap.put(entry.getKey(), 0);
        }
    	
    }
    
    @Override
    protected void cleanup(Context context){
    	pw.flush();
    	pw.close();
    }
}
