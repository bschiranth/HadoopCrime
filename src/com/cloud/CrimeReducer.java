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
import java.io.FileReader;
import java.util.Arrays;
public class CrimeReducer extends Reducer<Text,Text,Text,Text>{
	Map<String, Integer> crimeMap;
	Map<String, Integer> crimeIndexMap;
	StringBuilder sb;
	File newFile;
	PrintWriter pw;
	ArrayList<Long> ratingsList;
	long minCrimeRating = Long.MAX_VALUE, maxCrimeRating = Long.MIN_VALUE;
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
		
		crimeIndexMap = new HashMap<String, Integer>();
		crimeIndexMap.put("WARRANTS",7);
		crimeIndexMap.put("OTHER OFFENSES",5);
		crimeIndexMap.put("LARCENY/THEFT",3);
		crimeIndexMap.put("VEHICLE THEFT",4);
		crimeIndexMap.put("VANDALISM",4);
		crimeIndexMap.put("NON-CRIMINAL",1);
		crimeIndexMap.put("ROBBERY",5);
		crimeIndexMap.put("ASSAULT",6);
		crimeIndexMap.put("WEAPON LAWS",7);
		crimeIndexMap.put("BURGLARY",5);
		crimeIndexMap.put("SUSPICIOUS OCC",1);
		crimeIndexMap.put("DRUNKENNESS",2);
		crimeIndexMap.put("FORGERY/COUNTERFEITING",1);
		crimeIndexMap.put("DRUG/NARCOTIC",7);
		crimeIndexMap.put("STOLEN PROPERTY",2);
		crimeIndexMap.put("SECONDARY CODES",1);
		crimeIndexMap.put("TRESPASS",3);
		crimeIndexMap.put("MISSING PERSON",6);
		crimeIndexMap.put("FRAUD",6);
		crimeIndexMap.put("KIDNAPPING",8);
		crimeIndexMap.put("RUNAWAY",2);
		crimeIndexMap.put("DRIVING UNDER THE INFLUENCE",7);
		crimeIndexMap.put("SEX OFFENSES FORCIBLE",9);
		crimeIndexMap.put("PROSTITUTION",9);
		crimeIndexMap.put("DISORDERLY CONDUCT",1);
		crimeIndexMap.put("ARSON",8);
		crimeIndexMap.put("FAMILY OFFENSES",2);
		crimeIndexMap.put("LIQUOR LAWS",5);
		crimeIndexMap.put("BRIBERY",8);
		crimeIndexMap.put("EMBEZZLEMENT",7);
		crimeIndexMap.put("SUICIDE",5);
		crimeIndexMap.put("LOITERING",1);
		crimeIndexMap.put("SEX OFFENSES NON FORCIBLE",7);
		crimeIndexMap.put("EXTORTION",7);
		crimeIndexMap.put("GAMBLING",7);
		crimeIndexMap.put("BAD CHECKS",1);
		crimeIndexMap.put("TREA",3);
		crimeIndexMap.put("RECOVERED VEHICLE",3);
		crimeIndexMap.put("PORNOGRAPHY/OBSCENE MAT",4);
		ratingsList = new ArrayList<Long>();
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
    	long crimeRating = 0;
    	long totalCrimeCount = 0;
    	long totalCrimeResolved = 0;
    	double resolutionRate = 0.0;
    	long resolution = 0;
		//PrintWriter pw = new PrintWriter(newFile);
    	//long sum = 0;
    	for(Text val : values)
    	{
    		//sum += val.get();
    		String[] reducerValue = val.toString().split("-");
    		System.out.println(val);
    		crimeMap.put(reducerValue[0], crimeMap.get(reducerValue[0]) + 1);
    		++totalCrimeCount;
    		if(reducerValue[1].equals("RESOLVED"))
    			++totalCrimeResolved;
    		//context.write(new Text(key), val);
    	}
    	
    	resolutionRate = (double)totalCrimeResolved / totalCrimeCount;
    	resolution = totalCrimeCount - totalCrimeResolved;
    	System.out.println(totalCrimeCount + " " + totalCrimeResolved + " " + resolutionRate);
    	/*if(newFile.exists() && !newFile.isDirectory())
		{
    		sb.append(key.toString());
    		sb.append(",");
		}
    	*/
    	long individualRating = 0;
    	for(Map.Entry<String, Integer> entry : crimeMap.entrySet()) {
    		if(newFile.exists() && !newFile.isDirectory())
    		{
    			//sb.append(entry.getKey());
    			//sb.append("-->");
    			sb.append(entry.getValue());
    			sb.append(",");
    			crimeRating = crimeRating + (crimeMap.get(entry.getKey()) * crimeIndexMap.get(entry.getKey()));
    			 
    		}
    	}
    	crimeRating += resolution;
    	minCrimeRating = Math.min(minCrimeRating, crimeRating);
    	maxCrimeRating = Math.max(maxCrimeRating, crimeRating);
    	ratingsList.add(crimeRating);
    	sb.append("Resolutuion->" + resolutionRate);
    	sb.append(",");
    	sb.append(resolution);
    	sb.append(",");
    	sb.append(crimeRating);
    	sb.append(",");
    	
    	sb.append("\n");
    	pw.append(sb.toString());
	    context.write(key, new Text(sb.toString()));
    	sb.setLength(0);
    	for (Map.Entry<String, Integer> entry : crimeMap.entrySet()) {
            crimeMap.put(entry.getKey(), 0);
        }
    	
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException, FileNotFoundException {
    	float finalCrimeRating;
    	File ratingFile = new File("D:/Cloud/input/data/rating.csv");
    	PrintWriter writer = new PrintWriter(ratingFile);
    	//ArrayList<Long>
    	for(long val: ratingsList)
    	{
    		System.out.println("val: " + val + " Min: " + minCrimeRating + " Max: " + maxCrimeRating);
    		finalCrimeRating = (float)((val - minCrimeRating) * 10)/(maxCrimeRating - minCrimeRating);
    		System.out.println("FCR: " + finalCrimeRating);
    	}
    	pw.flush();
    	pw.close();
    }
}
