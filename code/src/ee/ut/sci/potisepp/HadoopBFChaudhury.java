package ee.ut.sci.potisepp;

import ij.IJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.PNG_Writer;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.FloatProcessor;
import ij.process.ShortProcessor;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.analysis.function.Gaussian;
import org.apache.commons.math3.analysis.function.Gaussian.Parametric;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HadoopBFChaudhury {
	
	public static class MyMap extends MapReduceBase implements Mapper<Text, BytesWritable, Text, BytesWritable>{
		
		//2*sigma_s+1 is the width of the kernel in pixels
		float sigma_s;
		//0...1.0
		float sigma_r;
		
//		private static final Log LOG = LogFactory.getLog(MyMap.class);
		
		public void configure(JobConf conf) {
			sigma_s = Float.valueOf(conf.get("sigma_s"));
			sigma_r = Float.valueOf(conf.get("sigma_r"));
	    }
		
		public void map(Text key, BytesWritable value, OutputCollector<Text, BytesWritable> output, Reporter reporter) throws IOException
		{							
			
			//extract info from the key
			String[] p = key.toString().split(";");
			int width = Integer.valueOf(p[1]);
			int height = Integer.valueOf(p[2]);
			int channels = Integer.valueOf(p[3]);
			int bpp = Integer.valueOf(p[4]);
			float min = Float.parseFloat(p[5]);
			float max = Float.parseFloat(p[6]);
			int numPixels = width*height;

			System.out.println("sigRange: " + sigma_r);
			System.out.println("sigmaX: " + sigma_s);
			System.out.println("sigmaY: " + sigma_s);
			System.out.println("sigmaZ: " + sigma_s);
			
			Filter filter = new Filter(sigma_r, sigma_s, sigma_s, sigma_s);
			
//			ImagePlus input = new ImagePlus(infile);
			
			//TODO implement class FloatArrayWritable
			//substitute FloatArrayWritable with Data
			
//			Data data = new Data(input);
					
			filter.setMultithread(true);
			filter.selectChannels(0, 0.0);
			int order[] = filter.computeOrder(min, max);
			int n = order[3]-order[2];
			if (n > 500)
				return;
			
			// --------------------------------------------------------------------------
			// Convert and process the input image
			// --------------------------------------------------------------------------
			int nx = width; //input.getWidth();
			int ny = height; //input.getHeight();
			int nz = channels; //input.getStackSize();
			int nxy = nx*ny;

			ImageStack stack = new ImageStack(nx, ny);
			
			long chrono = System.currentTimeMillis();
			
//			if (input.getType() == ImagePlus.COLOR_RGB) {
			if (channels == 3) {
				int rgb[][] = new int[nz][nxy];
				for(int c=0; c<3; c++) {
					float out[] = filter.execute(value, c);
					int shift = 8*(2-c);
					for(int z=0; z<nz; z++)
					for(int k=0; k<nxy; k++) 
						rgb[z][k] += (byte)(out[z*nxy+k]) << shift;
				}
				for(int z=0; z<nz; z++) {
					ColorProcessor cp = new ColorProcessor(nx, ny);
					cp.setPixels(rgb[z]);
					stack.addSlice("", cp);
				}
			}
			else {
				float out[] = filter.execute(value, 0);
//				if (input.getType() == ImagePlus.GRAY8) {
				if (channels == 1 && bpp == 8){
					for(int z=0; z<nz; z++) {
						byte gray[] = new byte[nxy];
						for(int x=0; x<nxy; x++)
							gray[x] = (byte)out[x + nxy*z];
						stack.addSlice("", new ByteProcessor(nx, ny, gray, null));
					}
				}
//				else if (input.getType() == ImagePlus.GRAY16) {
				else if (channels == 1 && bpp == 16){
					for(int z=0; z<nz; z++) {
						short gray[] = new short[nxy];
						for(int x=0; x<nxy; x++)
							gray[x] = (short)out[x + nxy*z];
						stack.addSlice("", new ShortProcessor(nx, ny, gray, null));
					}
				}
//				else if (input.getType() == ImagePlus.GRAY32) {
				else if (channels == 1 && bpp == 32){
					for(int z=0; z<nz; z++) {
						float gray[] = new float[nxy];
						for(int x=0; x<nxy; x++)
							gray[x] = out[x + nxy*z];
						stack.addSlice("", new FloatProcessor(nx, ny, gray, null));
					}
				}
			}
			
			System.out.println("Time (ms): " + (System.currentTimeMillis()-chrono) );
					
			if (stack.getSize() == channels) {
				ImagePlus impout = new ImagePlus("BFI on " + p[0] + " (" + sigma_s + "-" + sigma_r + ") ", stack);
//
//				PNG_Writer writer = new PNG_Writer();
//				
//				try {
//					writer.writeImage(impout, outfile, 0);
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
				
			}
			
			reporter.setStatus("done");
						
			output.collect(key, new BytesWritable(result));
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
		
		JobConf conf = new JobConf(HadoopBFChaudhury.class);
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
		
//		FileInputFormat.setInputPaths(conf, new Path("/home/karl/kool/msc_thesis/big_img/mapreduce_in"));
//		FileOutputFormat.setOutputPath(conf, new Path("/home/karl/kool/msc_thesis/big_img/mapreduce_out"));
		
//		FileInputFormat.setInputPaths(conf, new Path("/Users/karl/kool/msc_thesis/big_img/mapreduce_in"));
//		FileOutputFormat.setOutputPath(conf, new Path("/Users/karl/kool/msc_thesis/big_img/mapreduce_out"));
		
		JobClient.runJob(conf);
		
	}
	
}
