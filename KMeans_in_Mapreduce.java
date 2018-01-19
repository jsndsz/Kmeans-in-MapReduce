import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


import java.lang.Math;



public class Problem3dea {
	
	public enum MyCounters {
		Counter
	}

	public static Map<String,String> centroids = new HashMap <String,String>();
	public static Map<String,String> ccheck = new HashMap<String,String>();
	public static int k = 3;
	public static int i = 0;
	public static boolean convergence = false;
	
	
	public static class Problem3Mapper extends Mapper<Object,Text,Text,Text> {
		
		
		
		protected void setup(Context context) throws IOException, InterruptedException {
			
			
			Configuration conf = context.getConfiguration();
			
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			
			if(cacheFiles != null) {
				centroids.clear();
				BufferedReader bf = new BufferedReader(new FileReader(cacheFiles[0].toString()));
				String s;
				
				
				try
				{
				while((s = bf.readLine()) != null){
					String[] line = s.split(",");
					
					
					for (int i = 0 ; i < line.length;i++)
					{	
						if(line[i].equals("Center")) {
							centroids.put(line[i+1],line[i+2]);	
							break;
						}
						
					}
					
				}
				} finally {
					
					bf.close();
				}	
				
		
			}
			
		
			
		}
		
		String l1 ="";
		String l2="";
		int xsum = 0;
		int ysum = 0;
		
	public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
		
		
				
	
		String[] line = value.toString().split(",");
		double distance = 0;
		double allocate = Double.MAX_VALUE;
		Text center = new Text();
		Text point = new Text();
	
		List<Map.Entry<String,String>> cent = new ArrayList<Map.Entry<String,String>>(centroids.entrySet());
		
		l1 = line[1];
		l2 = line[2];
		

		for (Map.Entry<String,String> itr : cent) {
				
			distance = ((Integer.parseInt(itr.getKey()) - Integer.parseInt(l1))^2 +
							(Integer.parseInt(itr.getValue()) - Integer.parseInt(l2))^2 );
			
			if (distance <= allocate) {
				center = new Text(itr.getKey()+","+itr.getValue());
				point = new Text(l1+","+l2);
				allocate = distance;
			}		
			
		}
	
		context.write(center,point);

			
		}
	
	}
	

	public static class Problem3Combiner extends Reducer<Text,Text,Text,Text> {
		 
;
		int total = 0;
		Text newCenter = new Text();
		int counter = 0;
		

		public void reduce(Text key, Iterable<Text> values ,Context context) throws IOException, InterruptedException
		{ 
			int xSum = 0 , ySum = 0;
			
			for (Text x : values)  //iterate over values in a key
		      {
		        String[] line = x.toString().split(","); 
		        
		        xSum += Integer.parseInt(line[0]);
		        ySum += Integer.parseInt(line[1]);
		        counter +=1;
		      	
		    }
					
				
				
				context.write(key,new Text(String.valueOf(xSum/counter)+","+String.valueOf(ySum/counter)));
				counter = 0;
		}
	}
	
	public static class Problem3Reducer extends Reducer<Text,Text,Text,Text> {
		 
		
		String point = "Points";
		int total = 0;
		Text newCenter = new Text();
		

		public void reduce(Text key, Iterable<Text> values ,Context context) throws IOException, InterruptedException
		{ 
		
			
		int xSum = 0 , ySum = 0;
		int counter = 0;
		int newCenterX,newCenterY;
		String[] keyLine = key.toString().split(",");
		int keyX = Integer.parseInt(keyLine[0]);
		int keyY = Integer.parseInt(keyLine[1]);
	
		
			for (Text x : values)  //iterate over values in a key
		      {
		        String[] line = x.toString().split(","); 
		        
		        xSum += Integer.parseInt(line[0]);
		        ySum += Integer.parseInt(line[1]);
		        counter +=1;		
		        
		    }
				
			newCenterX = xSum/counter;
			newCenterY = ySum/counter;
			counter = 0;
			
			newCenter = new Text("Center,"+String.valueOf(newCenterX)+","+String.valueOf(newCenterY)+",");
			
			
			
			
			total = total+ Math.abs(keyX-newCenterX)+Math.abs(keyY-newCenterY);
			
			
			
			ccheck.put(("Center,"+String.valueOf(newCenterX)+","+String.valueOf(newCenterY)+","),"covergence,false");
	}			      
	
		protected void cleanup(Context context)
			      throws IOException, InterruptedException
			   
			      {
					List<Map.Entry<String,String>> clusters = new ArrayList<Map.Entry<String,String>>(ccheck.entrySet());
					if (total < 12000 ) {
						convergence = true;
						context.getCounter(MyCounters.Counter).increment(1);
						for (Map.Entry<String,String> itr : clusters) {
									context.write(new Text(itr.getKey()), new Text("convergence,true"));
								}
							}
					
					else
					{
						
						for (Map.Entry<String,String> itr : clusters) {
							context.write(new Text(itr.getKey()), new Text(itr.getValue()));
						}
						
						ccheck.clear();
						
						
						
					}
					
					
			      }
		
	}
	
	
	public static void main(String[] args) throws Exception {
		
		
		String kValue = args[0];
		Path kValPath = new Path(args[0]);
		Job job = new Job();
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		boolean isTrue = false;
		long counter;
		
			Configuration conf = new Configuration();  //Initialize a job configuration
		    job = new Job();
		    FileSystem fs = FileSystem.get(conf);
		     	
		    kValPath = new Path(args[0]);
		   
		    Path output = null;
		    
					
			DistributedCache.addCacheFile(kValPath.toUri(), job.getConfiguration());
		   
		    
		    output = new Path(args[2]+i);
			
			job.setJobName("Problem3");
			job.getInstance(conf,"Problem3");
			job.setJarByClass(Problem3dea.class);	
		
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
		
			job.setMapperClass(Problem3Mapper.class);
			job.setCombinerClass(Problem3Combiner.class);
			job.setReducerClass(Problem3Reducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		  
  
		  FileInputFormat.addInputPath(job, new Path(args[1]));  //Inputs from the command prompt
		  

		  FileOutputFormat.setOutputPath(job, output);

			
		  job.waitForCompletion(true);


		  kValPath = null;
		  kValPath = new Path(args[2]+i+"/part-r-00000");
		  
		  counter = job.getCounters().findCounter(MyCounters.Counter).getValue();
		  
		  isTrue = convergence;
		for(i =1; i <20; i++) {   
			
			if(isTrue==true || counter >0) {
				
				i = 1000;
				System.exit(0);
				break;
				
				
			}
			
			else {
			conf = new Configuration();  //Initialize a job configuration
		    job = new Job();
		    fs = FileSystem.get(conf);
		    
		    
		    output = null;
		    
					
			DistributedCache.addCacheFile(kValPath.toUri(), job.getConfiguration());
		   
		    
		    output = new Path(args[2]+i);
			
			job.setJobName("Problem3");
			job.getInstance(conf,"Problem3");
			job.setJarByClass(Problem3dea.class);	
		
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
		
			job.setMapperClass(Problem3Mapper.class);
			job.setCombinerClass(Problem3Combiner.class);
			job.setReducerClass(Problem3Reducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		  
  
		  FileInputFormat.addInputPath(job, new Path(args[1]));  //Inputs from the command prompt
		  

		  FileOutputFormat.setOutputPath(job, output);

			
		  job.waitForCompletion(true);


		  kValPath = null;
		  kValPath = new Path(args[2]+i+"/part-r-00000");
		  counter = job.getCounters().findCounter(MyCounters.Counter).getValue();
		  
		  
		  isTrue = convergence;
			}
        
		}
		}
		}

		
		

