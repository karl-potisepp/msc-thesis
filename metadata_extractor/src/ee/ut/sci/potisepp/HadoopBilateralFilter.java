package ee.ut.sci.potisepp;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HadoopBilateralFilter {
	
	public static class MyMap extends MapReduceBase implements Mapper<Text, BytesWritable, Text, BytesWritable>{
		
			
		public void map(Text key, BytesWritable value, OutputCollector<Text, BytesWritable> output, Reporter reporter) throws IOException
		{			
			
			//TODO extract PSNR 
			//http://bigwww.epfl.ch/sage/soft/snr/
			
			//find the box
								
//			output.collect(key, new BytesWritable(result));
			
		}
		
		
	}
	
	public static class MyReduce extends MapReduceBase implements Reducer<Text, BytesWritable, Text, BytesWritable>{		
		public void reduce(Text key, Iterator<BytesWritable> values, OutputCollector<Text, BytesWritable> output, Reporter reporter) throws IOException
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
		
		JobConf conf = new JobConf(HadoopBilateralFilter.class);
		conf.setJobName("hadoop_fast_bf");
		
		if(args.length==4){
			conf.setFloat("sigma_r", Float.valueOf(args[3]));
			conf.setInt("sigma_s", Integer.valueOf(args[2]));			
		}
		else{
			conf.setFloat("sigma_r", 0.15f);
			conf.setInt("sigma_s", 16);
		}		
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(BytesWritable.class);
		
		conf.setMapperClass(MyMap.class);
//		conf.setReducerClass(MyReduce.class);
		conf.setNumReduceTasks(0);
		
		conf.setInputFormat(MyInputFormat.class);
		conf.setOutputFormat(MyOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
	}
	
}
