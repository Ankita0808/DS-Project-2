import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
Donald Kline, Jr and Ankita Mohapatra
Indexing Class.
This was built from the shell provided on Apache's Map Reduce Tutorial Website
https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html
*/
public class Indexer
{

/**
The Map class for the Hadoop implementation.
 We originally had IntWritable, like in the tutorial, but we forgot the combiner and reducer needed the same signature, and the 
 reducer works better as Text even though the combiner worked better as IntWritable
*/
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
  { 
    private Text one_text = new Text("1");
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
    {
	try{
        String filename = ((FileSplit)reporter.getInputSplit()).getPath().getName();
        String line = value.toString();
        //Replace all of punctuation symbols with empty space. Otherwise, things like the phrase "did you eat yet?" Would not have a result for 'yet'
        line = line.replaceAll("[\\p{Punct}]", "");
        
        //The following lines were used from the Apache map reduce tutorial 
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) 
        {
        	String token = tokenizer.nextToken();
        	//Decided to store the inverted index as token:filename
            word.set(token.toLowerCase()+":"+filename);
            output.collect(word, one_text);
        }
	}catch(IOException e)
	{
		System.out.println(e+"\n");
		e.printStackTrace();
		throw e;
	}
    }
  } 
 
  /**
   Built off the apache example, and adjusted for the fact the key is the inverted index of word:File instead of just word
  */ 
  public static class OurCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
  {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			int sum=0;
			while (values.hasNext()) 
			{
				sum += Integer.parseInt(values.next().toString());
			}
			String word = key.toString().split(":")[0];
			String file = key.toString().split(":")[1];
 			output.collect(new Text(word), new Text(file+":"+sum));
 		}
 }

  /**
   Built off the apache example, and adjusted for the fact the key is the inverted index of word:File instead of just word
  */ 
  public static class OurReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
  {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> 
output, Reporter reporter) throws IOException 
		{

			String indexes="";
			if(values.hasNext())
				indexes=values.next().toString();
			while (values.hasNext()) 
			{
				indexes= indexes + ","+values.next().toString();
			}
 			output.collect(key, new Text(indexes));

 		}
 }
 

  	/**
	Launching of the Indexer, based off the Apache example
  	*/
 	public static void main(String[] args) throws Exception 
 	{
		JobConf conf = new JobConf(Indexer.class);
		conf.setJobName("wordcount");
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(OurCombiner.class);
		conf.setReducerClass(OurReducer.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
 	}
        
}

