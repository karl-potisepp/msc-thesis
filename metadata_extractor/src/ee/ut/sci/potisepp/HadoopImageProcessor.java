package ee.ut.sci.potisepp;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HadoopImageProcessor {
	
	public static class MyMap extends MapReduceBase implements Mapper<Text, ImageData, Text, Text>{
		
			
		public void map(Text key, ImageData value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
		{			
			System.out.println(key.toString());
			
			Map<String, String> metadata = value.getMetadata();
			Iterator<String> it = metadata.keySet().iterator();
			String cur_key;
			String outval = "";
			while(it.hasNext()){
				cur_key = it.next();
				outval += "\t" + cur_key + "\t" + metadata.get(cur_key);
//				System.out.println(cur_key + " : " + metadata.get(cur_key));
			}
				
			output.collect(key, new Text(outval));
			
		}
		
		
	}
	
	public static class MyReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
		{
			while(values.hasNext()){
				output.collect(key, values.next());
			}
		}		
	}

	public static void main(String[] args) throws Exception {
		
		if(args.length<2){
			System.out.println("Not enough arguments! Please provide input path and output path.");
			System.exit(0);
		}
		
		JobConf conf = new JobConf(HadoopImageProcessor.class);
		conf.setJobName("hadoop_metadata_extract");		
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(MyMap.class);
		conf.setReducerClass(MyReduce.class);
//		conf.setNumReduceTasks(0);
		
		conf.setInputFormat(MyInputFormat.class);
//		conf.setOutputFormat(MyOutputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]+"/*/*/*/*/*/*/*jpg"));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
	}
	
}
