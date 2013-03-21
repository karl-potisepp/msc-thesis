package ee.ut.sci.potisepp;

import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class MyImageWriter implements RecordWriter<Text, Data>{
	
	private FSDataOutputStream out;
	private JobConf conf;
	private FileSystem fs;
	private Progressable progress;
	
	public MyImageWriter(FileSystem fs, JobConf conf, String name, Progressable progress) throws IOException {
		this.fs = fs;
		this.progress = progress;
		this.conf = conf;
	}
	
	@Override
	public void close(Reporter arg0) throws IOException {
//		if(out!=null){
//			out.close();
//		}
	}

	@Override
	public void write(Text arg0, Data arg1) throws IOException {			
		
//		System.out.println("WRITE: " + arg0.toString());
		String[] parts = arg0.toString().split(";");
		String name = parts[0];
		int widthInPx = arg1.nx;
		int heightInPx = arg1.ny;
		int channels = arg1.nc;
		
		Path file = FileOutputFormat.getTaskOutputPath(conf, name);
		this.out = fs.create(file, progress);
				
		BufferedImage img;
		if (channels == 3) img = new BufferedImage(widthInPx, heightInPx, BufferedImage.TYPE_INT_RGB);
		else img = new BufferedImage(widthInPx, heightInPx, BufferedImage.TYPE_BYTE_GRAY);
		
		//convert from IntArrayWritable to int[x][y][3]
		float[][] raw = new float[channels][widthInPx * heightInPx];
		for (int i = 0; i < channels; i++){
			raw[i] = arg1.getPixels(i);
		}
		
		int rgb = 0;
//		int pixels = widthInPx * heightInPx;
		//System.out.println(" pixels: " + widthInPx*heightInPx);
		for(int x = 0; x < widthInPx; x++){
			for(int y = 0; y < heightInPx; y++){
				if( channels == 3){
					rgb = (((int) raw[0][x + y*widthInPx] & 0xff) << 16) | (((int) raw[1][x + y*widthInPx] & 0xff) << 8) | ((int) raw[2][x + y*widthInPx] & 0xff);	
				} else if ( channels == 1){
					rgb = (int) raw[0][x + y*widthInPx] & 0xff;					
				}
								
				img.setRGB(x, y, rgb);
			}
		}
				
		ImageIO.write(img, "png", out);
		out.flush();
		out.close();
		
	}		
}