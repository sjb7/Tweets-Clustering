package org;

import java.util.Set;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.ArrayList;
import java.text.DecimalFormat;

public class Point{
	
	/*This is method is going to calculate the distance between two tfidf vectors.
	  The dimension is going to be the number of elements in the union of keys of two tfidf vectors.
	  Union of keys of tfidf vectors are stored in the set and all the mappings for key and frequency are stored in two different 		  hash tables for each tfidf vector. 	
	  Then to calculate the Manhattan distance between two tfidf vectors , we calculate sum of absolute distance between two tfidf vectors 		  for each dimension. This distance is just the difference between the frequency. 
	*/

	 public static double getManhtDist(String tweet1,String tweet2)
	 {
		double dist=0.0;
		Hashtable<String, Double> tfidf1 = new Hashtable<String, Double>();
		Hashtable<String, Double> tfidf2 = new Hashtable<String, Double>();
		
		Set<String> dimensionSet = new HashSet<String>();
		
		tweet1 = Point.getTweetValue(tweet1);
		tweet2 = Point.getTweetValue(tweet2);
		
		String[] dimensions = tweet1.split(",");
		for(String dimension : dimensions )
		{
			String[] point = dimension.split(":");
			String key = point[0].trim();
			dimensionSet.add(key);
			tfidf1.put(key,Double.parseDouble(point[1]));
			tfidf2.put(key,0.0);
		}
		
		dimensions = tweet2.split(",");
		for(String dimension : dimensions )
		{
			String[] point = dimension.split(":");
			String key = point[0].trim();
			dimensionSet.add(key);
			tfidf2.put(key,Double.parseDouble(point[1]));
			if(!tfidf1.containsKey(key)) 
				tfidf1.put(key,0.0);

		}
		for(String key : dimensionSet)	
			dist += Math.abs(tfidf1.get(key) - tfidf2.get(key));
  
		return dist;
	 }
	
	/* In our tfidf-vectors.txt , the values of frequency for words are stored inside curly braces and there is a value on each and every 		   line. This method returns a Srings containing value of the tfidf vector. 
	*/	 
	public static String getTweetValue(String vec)
	{
		if(vec.contains("{") && vec.contains("}"))
		{
			vec  = vec.split("\\{")[1];
			vec  = vec.split("\\}")[0];
			
		}
		return vec;
	}

	/* For each tfidf vector string add the vector to a sum vector which has a dimension of the union of dimensions of all tfidf vectors
	   Converts the sum vector into a string and returns it.	
	*/ 
	public static String getSum(String... args)
	{	
		Set<String> dimensionSet = new HashSet<String>();
		Hashtable<String, Double> sumVec = new Hashtable<String, Double>();
		StringBuilder sum = new StringBuilder();
		DecimalFormat df = new DecimalFormat("0.0000");
		
		for(String vec : args)
		{
			vec = Point.getTweetValue(vec);
			String[] dimensions = vec.split(",");
			for(String dimension : dimensions )
			{
				String[] point = dimension.split(":");
				String key = point[0].trim();
				dimensionSet.add(key);
				if(!sumVec.containsKey(key))
				{
					sumVec.put(key,Double.parseDouble(point[1]));
				}
				else
				{
					sumVec.put(key,sumVec.get(key)+Double.parseDouble(point[1]));
				}
			}
		}
	
		for(String key : dimensionSet)
		{
			sum.append(key+":"+df.format(sumVec.get(key)) + ",");
        	}
		if(sum.length() > 0)
		{
			sum.setLength(sum.length() - 1);
		}
		return sum.toString();
	}
	

/* It is used to get the new centroid point. The sum of individual dimensions for a cluster is taken out by the above method. In this mehod we 
   we divide each dimension with the total count of tfidf vectors in the cluster and get the dimension of new centroid.
*/
public static String getNewCentroid(String vectorSum, int count)
{
		StringBuilder sum = new StringBuilder();
		DecimalFormat df = new DecimalFormat("0.0000");
		
		if(!vectorSum.equals(""))
		{
			String[] dimensions = vectorSum.split(",");
			for(String dimension : dimensions )
			{
				String[] point = dimension.split(":");
				String key = point[0].trim();
				sum.append(key+":"+df.format(Double.parseDouble(point[1])/count) + ",");
			}
		}
		if(sum.length() > 0)
			sum.setLength(sum.length() - 1);
		return sum.toString();
}


/* This method is used to calculate the key for the key value pair which is going to be the centroid  
*/
public static String getTweetIndex(String vec) {
		String index  = vec.split(":")[0];
		return index.trim();
}

public static String getTopTerms(String vec, int count)
	{
		StringBuilder topTerms = new StringBuilder();
		Set<String> dimensionSet = new HashSet<String>();
		Hashtable<String, Double> htVec = new Hashtable<String, Double>();
		DecimalFormat df = new DecimalFormat("0.0000");
		
		vec = Point.getTweetValue(vec);
		String[] dimensions = vec.split(",");
		for(String dimension : dimensions )
		{
			String[] point = dimension.split(":");
			String key = point[0].trim();
			dimensionSet.add(key);
			if(!htVec.containsKey(key))
			{
				htVec.put(key,Double.parseDouble(point[1]));
			}
			else
			{
				htVec.put(key,htVec.get(key)+Double.parseDouble(point[1]));
			}
		}
		
		int topCount = dimensions.length > count ? count : dimensions.length;

		for(int i = 0; i < topCount; i++)
		{
			String index = "";
			double value = Double.MIN_VALUE;
			for(String key : dimensionSet)
			{
				if(index.equals(""))
				{
					index = key;
					value = htVec.get(key);
				}
				else
				{
					if(value < htVec.get(key))
					{
						index = key;
						value = htVec.get(key);
					}
				}
			}
			
			dimensionSet.remove(index);
			topTerms.append(index+":"+df.format(htVec.get(index)) + ",");
		}
		if(topTerms.length() > 0)
		{
			topTerms.setLength(topTerms.length() - 1);
		}

		return topTerms.toString();
	}
}

