package ee.ut.sci.potisepp;


import java.io.BufferedInputStream;
//import java.io.BufferedReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.io.InputStreamReader;
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
//			HashSet<String> tags = new HashSet<String>();
//			tags.add("Caption Digest");
			
			Text outkey = new Text();
			IntWritable outval = new IntWritable(1);
			
			for (Directory directory : metadata.getDirectories()) {
			    for (Tag tag : directory.getTags()) {
//			    	if( tags.contains(tag.getTagName()) ){
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
	
public static class OCRMap extends MapReduceBase implements Mapper<Text, BytesWritable, Text, Text>{
		
		JobConf conf;
		FileSystem fs;
		String pathToObj;
		String pathToScript;
		String pathToFindObj;
		String pathToFindBox;
		String pathToExtractBox;
		
		public void configure(JobConf conf) {
			this.conf = conf;
			pathToObj = conf.get("pathToObj");
			pathToScript = conf.get("pathToScript");
			pathToFindObj = conf.get("pathToFindObj");
			pathToFindBox = conf.get("pathToFindBox");
			pathToExtractBox = conf.get("pathToExtractBox");
			Path p = new Path("/tmp/");
			
			try {
				fs = p.getFileSystem(conf);

//				FileUtils.copyFile(new File(pathToObj), new File("/tmp/obj.png"));
//				FileUtils.copyFile(new File(pathToScript), new File("/tmp/pandore_script.sh"));
//				FileUtils.copyFile(new File(pathToFindObj), new File("/tmp/find_obj"));
//				FileUtils.copyFile(new File(pathToFindBox), new File("/tmp/find_box.py"));
//				FileUtils.copyFile(new File(pathToExtractBox), new File("/tmp/extract_box.py"));
				
//				fs.copyToLocalFile(new Path(pathToObj), new Path("/tmp/obj.png"));
//				fs.copyToLocalFile(new Path(pathToScript), new Path("/tmp/pandore_script.sh"));
//				fs.copyToLocalFile(new Path(pathToFindObj), new Path("/tmp/find_obj"));
//				fs.copyToLocalFile(new Path(pathToFindBox), new Path("/tmp/find_box.py"));
//				fs.copyToLocalFile(new Path(pathToExtractBox), new Path("/tmp/extract_box.py"));
												
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
			
	    }
		
		public void close(JobConf conf){
			try {
				fs.delete(new Path("/tmp/obj.png"), false);
				fs.delete(new Path("/tmp/pandore_script.sh"), false);
				fs.delete(new Path("/tmp/find_obj.py"), false);
				fs.delete(new Path("/tmp/find_box.py"), false);
				fs.delete(new Path("/tmp/extract_box.py"), false);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
			
		public void map(Text key, BytesWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
		{			
			String keyStr = key.toString();
			String tmpFilename = "/tmp/imageprocessor-"+UUID.randomUUID().toString();
			File tmpFile = new File(tmpFilename);
			FileUtils.writeByteArrayToFile(tmpFile, value.getBytes());
			
//			FSDataInputStream fsin = fs.open(new Path(tmpFilename));
			BufferedInputStream bin = new BufferedInputStream(FileUtils.openInputStream(tmpFile));
			
			Metadata metadata = null;
			TreeMap <String, String> metadata_map = new TreeMap<String, String>();
			HashSet<String> tags = new HashSet<String>();
			
			if(keyStr.toLowerCase().endsWith("jpeg") || keyStr.toLowerCase().endsWith("jpg")){
						
				try {
					metadata = ImageMetadataReader.readMetadata(bin, true);
					tags.add("Image Height");
					tags.add("Image Width");
					tags.add("Date/Time");
					tags.add("City");
					tags.add("Copyright");
					tags.add("Credit");
					
					for (Directory directory : metadata.getDirectories()) {
					    for (Tag tag : directory.getTags()) {
					    	if( tags.contains(tag.getTagName()) ){
					    		metadata_map.put(tag.getTagName(), tag.getDescription());
					    	}		        
					    }
					}
					
				} catch (ImageProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
				
			bin.close();
//			fsin.close();
			
			Runtime r = Runtime.getRuntime();
			Process p = r.exec("/home/ubuntu/ocr/pandore_script.sh "+ pathToObj + " " + tmpFilename, null, new File("/home/ubuntu/ocr"));
			int retval = 0;
			try {
				retval = p.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

//			uncomment to see script stdout
			
			//stdout
			BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			
			while ((line = b.readLine()) != null) {
			  System.out.println(line);
			}
			
			//stderr
			b = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while ((line = b.readLine()) != null) {
			  System.err.println(line);
			}
			
			if( retval == 0){
				metadata_map.put("OCR", "SUCCESS");
//				outkey.set("SUCCESS");
//	    		output.collect(outkey, outval);
				//read OCR outfile and delete it afterwards
	    		File OCRFile = new File(tmpFilename+".txt");
	    		String OCRtext = "";
	    		
	    		try {
	    			Scanner OCRScanner = new Scanner(FileUtils.openInputStream(OCRFile)).useDelimiter("\n");
		    		while(OCRScanner.hasNext()){
		    			OCRtext += " " + OCRScanner.next();
		    		}
		    		
		    		if(OCRtext.trim().length() > 0)	metadata_map.put("OCR_RESULT", OCRtext);
		    		
					FileUtils.deleteQuietly(OCRFile);
	    		}
	    		catch ( FileNotFoundException e){
	    			System.err.println("OCR result file not found.");
	    		}	    		
	    		 		
			}
			else if (retval == 255){
				metadata_map.put("OCR", "NO MATCH (FIND OBJ)");
//				outkey.set("NO MATCH (FIND OBJ)");
//	    		output.collect(outkey, outval);
			}
			else if (retval == 254){
				metadata_map.put("OCR", "NO MATCH (EXTRACT ROI)");
//				outkey.set("NO MATCH (EXTRACT ROI)");
//	    		output.collect(outkey, outval);
			}
			
			FileUtils.deleteQuietly(tmpFile);
			p = r.exec("rm "+ tmpFilename, null, new File("/tmp"));
			try {
				retval = p.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
						
//			Map<String, String> metadata = image.getMetadata();
			Iterator<String> it = metadata_map.keySet().iterator();
			String cur_key;
			Text outval = new Text("");
			while(it.hasNext()){
				cur_key = it.next();
				outval.set(cur_key + ";" + metadata_map.get(cur_key));
//				System.out.println(cur_key + " : " + metadata.get(cur_key));
				output.collect(key, new Text(outval));
			}
						
		}
	}
	
	public static class OCRReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
		{
			
			ArrayList<String> data = new ArrayList<String>();
			while(values.hasNext()){
				data.add(values.next().toString());
			}
			
			Collections.sort(data);
			
			Iterator<String> it = data.iterator();
			String tmp = "";
			while(it.hasNext()){
				tmp += it.next()+"\t";
			}
			
			output.collect(key, new Text(tmp));
			
//			int sum = 0;
//			
//			while(values.hasNext()){
//				sum += values.next().get();
//			}
//			output.collect(key, new IntWritable(sum));
			
		}		
	}

	public static void main(String[] args) throws Exception {
		
		if(args.length<7){
			System.out.println("Not enough arguments! Required: INPUT_PATH OUTPUT_PATH BOX_PNG PANDORE_SCRIPT FIND_OBJ FIND_BOX EXTRACT_BOX");
			System.exit(0);
		}
		
		JobConf conf = new JobConf(HadoopImageProcessor.class);
		conf.setJobName("hadoop_metadata_extract");
		
//		conf.set("pathToObj", "/home/karl/kool/msc_thesis/git/object_recognition_test/box.png");
//		conf.set("pathToScript", "/home/karl/kool/msc_thesis/git/object_recognition_test/pandore_script.sh");
//		conf.set("pathToFindObj", "/home/karl/kool/msc_thesis/git/object_recognition_test/find_obj");
//		conf.set("pathToFindBox", "/home/karl/kool/msc_thesis/git/object_recognition_test/find_box.py");
//		conf.set("pathToExtractBox", "/home/karl/kool/msc_thesis/git/object_recognition_test/extract_box.py");
		
		conf.set("pathToObj", args[2]);
		conf.set("pathToScript", args[3]);
		conf.set("pathToFindObj", args[4]);
		conf.set("pathToFindBox", args[5]);
		conf.set("pathToExtractBox", args[6]);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(OCRMap.class);
		conf.setReducerClass(OCRReduce.class);
//		conf.setNumReduceTasks(0);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
//		conf.setOutputFormat(MyOutputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
	}
	
}
