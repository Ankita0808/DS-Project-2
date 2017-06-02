import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MiniGoogle{

  public static class OurMapper extends Mapper<Object, Text, Text, Text> {
	private Text outKey = new Text();

    @Override
    public void map (Object key,Text value,Context context) throws InterruptedException, IOException
    {
    	Configuration our_conf = context.getConfiguration();
    	String keywords = our_conf.get("search_terms");
		String [] index_value = value.toString().split("\t");//Organized like: key\tbook1:count1,book2:count2, etc...
		String [] keyword_arr = keywords.split(":");
		boolean has_key = false;
		for(int i=0;i<keyword_arr.length;i++){
			if(index_value[0].equals(keyword_arr[i])){
				has_key=true;
			}
		}
	//Use & to split the values, since : is already taken
	if (has_key)
		context.write(new Text("OurKeys"), new Text(index_value[0]+"&"+index_value[1].toString()) );
    }
  }
  
  public static class OurReducer extends Reducer<Text, Text, Text, Text> 
  {
	    @Override
	    public void reduce (Text key,Iterable<Text> values,Context context) throws InterruptedException, IOException  {
	    	try{
			//Originally had HashMap, but ran into some issues so switched to Hashtable
			Hashtable<String, Integer> countOfWords = new Hashtable<String, Integer>();
			Hashtable<String, Hashtable<String, Integer> > containsAllVerify = new Hashtable<String, Hashtable<String,Integer> >();
	    	ArrayList<String> orderedFileNames = new ArrayList<String>();
	    	ArrayList<String> searchNames = new ArrayList<String>();
	    	while (values.iterator().hasNext()) 
			{
	    		String[] foundItems = values.iterator().next().toString().split("&");
	    		if(!searchNames.contains(foundItems[0])){
	    			searchNames.add(foundItems[0]); 
	    		}
				
	    		String[] keys = foundItems[1].split(":|,");
				for(int i=0;i<keys.length;i+=2){
					if(!orderedFileNames.contains(keys[i])){
						orderedFileNames.add(keys[i]);
						countOfWords.put(keys[i],0);
					}
					if (Integer.parseInt(keys[i+1])>0)
					{
						

						Hashtable<String,Integer> currentSet = new Hashtable<String,Integer>();
						if (containsAllVerify.containsKey(keys[i]))
						{
							currentSet=containsAllVerify.get(keys[i]);
						}
						int current_sum = 0;
						if (currentSet.containsKey(foundItems[0]))
							current_sum = currentSet.get(foundItems[0]);
						current_sum += Integer.parseInt(keys[i+1]);
						currentSet.put(foundItems[0],current_sum);
						containsAllVerify.put(keys[i],currentSet);
					} 
					int running_total = countOfWords.get(keys[i]);
					running_total += Integer.parseInt(keys[i+1]);
					countOfWords.put(keys[i], running_total);
				}
			}
	    	
			//Sort the ordered filenames
	    	for(int i=0;i<orderedFileNames.size()-1;i++){
				for(int j=i+1;j<orderedFileNames.size();j++){
					if(countOfWords.get(orderedFileNames.get(i))<countOfWords.get(orderedFileNames.get(j)))
					{
						//Swap positions
						String str = orderedFileNames.get(i); 
						orderedFileNames.set(i, orderedFileNames.get(j)); 
						orderedFileNames.set(j,str);
					}
				}
			}
	    	
	    	StringBuilder toReturn1 = new StringBuilder("Results when for searching for ");
	    	for(int i=0;i<searchNames.size();i++){
	    		toReturn1.append(searchNames.get(i)+",");
	    	}
			toReturn1.append(":");
	    	
	    	StringBuilder toReturn2=new StringBuilder("\n");
	    	for(int i=0;i<orderedFileNames.size();i++){
		    	boolean good = true;
				for(int j=0;j<searchNames.size();j++){
					if (containsAllVerify.get(orderedFileNames.get(i)).containsKey(searchNames.get(j)))
						good = good && (containsAllVerify.get(orderedFileNames.get(i)).get(searchNames.get(j))>0);
					else
					{
						good =false;
						break;
					}
				}			
				if (good)
				{
					String full = " respectively, (";
					for (int k=0; k<searchNames.size();k++)
						full= full + containsAllVerify.get(orderedFileNames.get(i)).get(searchNames.get(k))+", ";
					full=full+")";
	    			toReturn2.append("\n"+ orderedFileNames.get(i) + ": "+countOfWords.get(orderedFileNames.get(i))+":"+full);
				}
	    	}
	    	context.write(new Text(toReturn1.toString()), new Text(toReturn2.toString()) );
			}catch(InterruptedException e1){
				System.out.println(e1);
				e1.printStackTrace();
				throw e1;
			}catch(IOException e2){
				System.out.println(e2);
				e2.printStackTrace();
				throw e2;
			}catch(Exception e3){
				System.out.println(e3);
				e3.printStackTrace();
				throw new RuntimeException(e3.getMessage());
			}
	    }
	  }
 
  public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException 
  { 
    Configuration conf = new Configuration();
    String search_terms = new String();
    for(int i=2;i<args.length;++i){
    	search_terms = search_terms + ":"+ args[i];
    }
    conf.set("search_terms", search_terms);
    Job job = new Job(conf,"tinygoogle");
    
    job.setJarByClass(MiniGoogle.class);
    job.setMapperClass(OurMapper.class);
    job.setReducerClass(OurReducer.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    

    System.exit(job.waitForCompletion(true)?0:1);
    
  }
}
