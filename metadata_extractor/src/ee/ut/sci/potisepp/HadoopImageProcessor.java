package ee.ut.sci.potisepp;


import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;

public class HadoopImageProcessor {
	
	public static class MyMap extends MapReduceBase implements Mapper<Text, BytesWritable, Text, IntWritable>{
		
		JobConf conf;
		
		public void configure(JobConf conf) {
			this.conf = conf;
	    }
			
		public void map(Text key, BytesWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{			
//			String keyStr = key.toString();
			String tmpFilename = "/tmp/imageprocessor-"+UUID.randomUUID().toString();
			File tmpFile = new File(tmpFilename);
			FileUtils.writeByteArrayToFile(tmpFile, value.getBytes());
			
			Path p = new Path("/tmp/");
			FileSystem fs = p.getFileSystem(conf);
			FSDataInputStream fsin = fs.open(new Path(tmpFilename));
			BufferedInputStream bin = new BufferedInputStream(fsin);
			
			Metadata metadata = null;
			try {
				metadata = ImageMetadataReader.readMetadata(bin, true);
			} catch (ImageProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
//			TreeMap <String, String> metadata_map = new TreeMap<String, String>();
//			HashSet<String> ignore_tags = new HashSet<String>();
			
			Text outkey = new Text();
			IntWritable outval = new IntWritable(1);
			
			for (Directory directory : metadata.getDirectories()) {
			    for (Tag tag : directory.getTags()) {
//			    	if( !ignore_tags.contains(tag.getTagName()) ){
			    		//metadata_map.put(tag.getTagName(), tag.getDescription());
			    		outkey.set(tag.getTagName());
			    		output.collect(outkey, outval);
//			    	}		        
			    }
			}
			bin.close();
			fsin.close();
			
			FileUtils.deleteQuietly(tmpFile);
			
//			Map<String, String> metadata = image.getMetadata();
//			Iterator<String> it = metadata.keySet().iterator();
//			String cur_key;
//			String outval = "";
//			while(it.hasNext()){
//				cur_key = it.next();
//				outval += "\t" + cur_key + "\t" + metadata.get(cur_key);
////				System.out.println(cur_key + " : " + metadata.get(cur_key));
//			}
//				
//			output.collect(key, new Text(outval));
			
		}
		
		
	}
	
	public static class MyReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{
			int sum = 0;
			
			while(values.hasNext()){
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
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
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(MyMap.class);
		conf.setReducerClass(MyReduce.class);
//		conf.setNumReduceTasks(0);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
//		conf.setOutputFormat(MyOutputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
	}
	
}
