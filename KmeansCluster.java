package org;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

public class KmeansCluster extends Configured implements Tool
{
	// iteration Control
	private static final int MAXITERATIONS = 2;
	
	public static void updateCentroids(Configuration conf) throws IOException 
	{
		FileSystem fs = FileSystem.get(conf);
		Path pervCenterFile = new Path("centroids");
		Path currentCenterFile = new Path("tmpFolder/part-r-00000");
		if(!(fs.exists(pervCenterFile) && fs.exists(currentCenterFile)))
		{
			System.exit(1);
		}
		
		// delete pervCenterFile, then rename the currentCenterFile to pervCenterFile
		/* Why rename needed here: 
		 * as each iteration, the mapper will get centroid from Path("input/initK")
		 * but reducer will output new centroids into Path("output/newCentroid/part-r-00000")
		 */ 
		fs.delete(pervCenterFile,true);
		if(fs.rename(currentCenterFile, pervCenterFile) == false)
		{
			System.exit(1);
		}
		
	}
	 
	// This is my mapper class and the output of this file is going to be <Centroid,Point>
	public static class ClusterMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		Vector<String> centers = new Vector<String>();
		int k = 0;

		/* Centroid points to be used in the first iteration are being provided by me in the input/initK file and I am going to load 			   this values inside the centers Vector array */
		@Override
		public void setup(Context context)
		{
			try
			{
				/* DistributedCache is a facility provided by the Map-Reduce framework to cache files (text, archives, jars 					    etc.) needed by applications. */
				Path[] caches=DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if(caches == null || caches.length <= 0)
				{
					System.exit(1);
				}
				
				BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));
				String line;
				while((line=br.readLine()) != null)
				{
					centers.add(line);
					k++;		   
				}
				br.close();
			}
			catch(Exception e){}
		}

		@Override
		// This is the mapping method used by the mapper class
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			
			int index = -1;
			double minDist = Double.MAX_VALUE;
			String tweetVector = value.toString().trim();
			

			// find the nearest centroid to this point
			for(int i = 0;i < k; i++)
			{
				double dist = Point.getManhtDist(tweetVector, centers.get(i));
				if(dist < minDist)
				{
					minDist = dist;
					index = i;
				}
			}
			// output the nearest centroid as key, and the point as value
			context.write(new Text(centers.get(index)), new Text(tweetVector));
		}
	
	}
	
	/* This is the aggregation that is done on the local site. Since we can calculate the sum of the dimensions of tfidf vectors which 		   have the same centroid so we are going to do it and add an extra field to value which is the count of vectors with same key. This 		   saves us the amount of data to be shared between different nodes */
	public static class Combiner extends Reducer<Text, Text, Text, Text> 
	{	  
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			String outputValue;
			String sumStr = "";
			int count = 0;
			while(values.iterator().hasNext())
			{
				String line = values.iterator().next().toString();
				sumStr = Point.getSum(sumStr, line);
				count++;

			}
			
			outputValue = sumStr + "-" + String.valueOf(count);  //value=Point_Sum+count
			context.write(key, new Text(outputValue));
		}
	}

	/* This is my global reducer whose job is to calculate the new centroid. If the centroid does not changes than this is going to give 		   value 0 or else it is going to give value as 1*/ 
	public static class UpdateCenterReducer extends Reducer<Text, Text, Text, Text> 
	{ 
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			int count = 0;
			String sumStr = "";
			String newCentroid = "";

			// while loop to calculate the sum of points
			while(values.iterator().hasNext())
			{
				String line = values.iterator().next().toString();
				String[] str = line.split("-");
				count += Integer.parseInt(str[1]);
				sumStr = Point.getSum(sumStr, str[0]);
			}

			// calculate the new centroid
			newCentroid = Point.getNewCentroid(sumStr, count);
			newCentroid = Point.getTopTerms(newCentroid, 30);

			context.write(new Text(newCentroid),new Text());
			
		}
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf);
		job.setJarByClass(KmeansCluster.class);
		
		FileInputFormat.setInputPaths(job, "tfidf-vectors");
		Path outDir = new Path("tmpFolder"); // This is were the newCentroids are going to get stored
		fs.delete(outDir,true);
		FileOutputFormat.setOutputPath(job, outDir);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(ClusterMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(UpdateCenterReducer.class);
		job.setNumReduceTasks(1); // All the new centroids will get output into one file
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		 
		return job.waitForCompletion(true)?0:1;
	}
	
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		// set the path for cache, which will be loaded in ClusterMapper
		Path dataFile = new Path("centroids");
		DistributedCache.addCacheFile(dataFile.toUri(), conf);
 
		int iteration = 0;
		int success = 1;

		//This is loop is going to run for MAXITERATIONS time.
		do 
		{
			success ^= ToolRunner.run(conf, new KmeansCluster(), args);
			updateCentroids(conf);
			iteration++;
		} while (success == 1  && iteration < MAXITERATIONS ); 

		// This one is for the final output where we are going to get the clustered points with the centroid
		
		Job job = new Job(conf);
		job.setJarByClass(KmeansCluster.class);
		
		FileInputFormat.setInputPaths(job, "tfidf-vectors");
		Path outDir = new Path("finalCluster");
		fs.delete(outDir,true);
		FileOutputFormat.setOutputPath(job, outDir);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(ClusterMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		 
		job.waitForCompletion(true);
		
	}
}
