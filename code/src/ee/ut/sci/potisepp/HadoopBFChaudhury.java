package ee.ut.sci.potisepp;

import ij.IJ;

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
		int sigma_s;
		//0...1.0
		double sigma_r;
		
//		private static final Log LOG = LogFactory.getLog(MyMap.class);
		
		public void configure(JobConf conf) {
			sigma_s = Integer.valueOf(conf.get("sigma_s"));
			sigma_r = Double.valueOf(conf.get("sigma_r"));
	    }
		
		public void map(Text key, BytesWritable value, OutputCollector<Text, BytesWritable> output, Reporter reporter) throws IOException
		{							
			
			Filter bf = new Filter( (float) sigma_s, (float) sigma_r, (float) sigma_r, (float) sigma_r);
						
			//extract height & width from the key
			String[] parts = key.toString().split(";");
			int width = Integer.valueOf(parts[1]);
			int height = Integer.valueOf(parts[2]);
			int numPixels = width*height;
						
			//define a gaussian function and parameters for the pixel value range gaussian
			Parametric g = new Gaussian.Parametric();
			double[] gParams = { 1.0 / (Math.sqrt(2*Math.PI)*sigma_r), 0.0, sigma_r };			
			
			//discrete 2-dimensional gaussian for calculating weight by pixel location
			double[][] testg = generateDiscreteGaussian(sigma_s, sigma_s);
			
			//input
			byte[] pixels = value.getBytes();
			//output
			byte[] result = new byte[width * height * 3];
			
			//temporary variables
			int tmp_x, tmp_y;
			double w;
			float[] I_p = new float[3];
			float[] I_q = new float[3];
			float R_x_y, G_x_y, B_x_y, R_w, G_w, B_w;
			
			long progressCounter = 0;
			reporter.setStatus("(Reporter) sigma_r = " + sigma_r);
			reporter.setStatus("(Reporter) sigma_s = " + sigma_s);
			
			//for each pixel p in S
			for(int x = 0; x < width; x++){
				for(int y = 0; y < height; y++){
					
					progressCounter++;
					if(progressCounter%(Math.floor(numPixels/1000))==0){
						reporter.setStatus("(Reporter) processing: " + progressCounter + " / " + numPixels);
						//LOG.info("(LOG.info) processing: " + progressCounter + " / " + numPixels);
						//System.out.println("(stdout) processing: " + progressCounter + " / " + numPixels);
					}					
					
					R_w = 0.0f;
					G_w = 0.0f;
					B_w = 0.0f;
					R_x_y = 0.0f;
					G_x_y = 0.0f;
					B_x_y = 0.0f;
					
					I_p[0] = ((int) pixels[x + y*width] & 0xff) / 255.0f;
					I_p[1] = ((int) pixels[x + y*width + numPixels] & 0xff) / 255.0f;
					I_p[2] = ((int) pixels[x + y*width + 2*numPixels] & 0xff) / 255.0f;
					
					//for each pixel q in the kernelSize-neighborhood of p in S
					for(int kx = 0; kx < 2*sigma_s + 1; kx++){
						
						tmp_x = x - sigma_s + kx;
						if (tmp_x < 0) tmp_x = 0;
						else if (tmp_x > width - 1) tmp_x = width - 1;
						
						for(int ky = 0; ky < 2*sigma_s + 1; ky++){
							
							tmp_y = y - sigma_s + ky;
							if (tmp_y < 0) tmp_y = 0;
							else if (tmp_y > height - 1) tmp_y = height - 1; 
							
							I_q[0] = ((int) pixels[tmp_x + tmp_y*width] & 0xff) / 255.0f;
							I_q[1] = ((int) pixels[tmp_x + tmp_y*width + numPixels] & 0xff) / 255.0f;
							I_q[2] = ((int) pixels[tmp_x + tmp_y*width + 2*numPixels] & 0xff) / 255.0f;
							
							w = testg[kx][ky] * g.value(dist(I_p,I_q), gParams); 
							
							R_x_y += w*I_q[0];
							R_w += w;
							G_x_y += w*I_q[1];
							G_w += w;
							B_x_y += w*I_q[2];
							B_w += w;
													
						}
					}
					
					//normalize, convert to 0-255 scale, and write to result
					result[x + y*width] = (byte) ((R_x_y / R_w) * 255.0f);
					result[x + y*width + numPixels] = (byte) ( (G_x_y / G_w) * 255.0f);
					result[x + y*width + 2*numPixels] = (byte) ( (B_x_y / B_w) * 255.0f);
				}				
			}
			
			reporter.setStatus("done");
						
			output.collect(key, new BytesWritable(result));
		}
		
		//euclidean distance
		private double dist(float[] a, float[] b){			
			double ret = 0.0;			
			for(int i = 0; i < a.length; i++){
				ret += Math.pow(a[i]-b[i], 2);
			}						
			return Math.abs( Math.sqrt(ret) );			
		}
		
		private double[][] generateDiscreteGaussian(double sigmax, double sigmay) {
			int nx = 2*(int)Math.ceil(sigmax)+1;
			int ny = 2*(int)Math.ceil(sigmay)+1;
			int hx = nx/2;
			int hy = ny/2;
			double g[][] = new double[nx][ny];
			double s = 1.0 / (sigmax*sigmay*2);
			double cst = 1.0 / (2*Math.PI*sigmax*sigmay);
			for (int x=0; x<nx; x++)
			for (int y=0; y<ny; y++)
				g[x][y] = cst*Math.exp(-((x-hx)*(x-hx)+(y-hy)*(y-hy))*s);
			return g;
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
