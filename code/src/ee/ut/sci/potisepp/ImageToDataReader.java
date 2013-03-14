package ee.ut.sci.potisepp;

import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class ImageToDataReader implements RecordReader<Text, BytesWritable>{
	
	private Text key = new Text();
//	private IntArrayWritable value = new IntArrayWritable();
	private BytesWritable value = new BytesWritable();
	private boolean read = false;
			
	int counter = 0;
	private float progress = 0.0f;
	
	BufferedImage img;
	FileSplit split;
	Path path;
	FileSystem fs;
	FSDataInputStream fsin;
	
	public ImageToDataReader(Configuration jobConf, InputSplit inputSplit) throws IOException {
		
		//we assume here that the image is not split in such a way that it's unreadable :)
		split = (FileSplit) inputSplit;
		path = split.getPath();
		fs = path.getFileSystem(jobConf);						
		FSDataInputStream fsin = fs.open(path);
		
		//TODO read with ImageJ
		
		img = ImageIO.read(fsin);
		int width = img.getWidth();
		int height = img.getHeight();
		//we assume the image has 3 channels - red, green and blue
		int channels = 3;
		int pixels = width * height;
		
		byte[] values = new byte[width * height * channels];
				
		//set key to the form of "IMAGE_NAME.EXTENSION;WIDTH_PIXELS;HEIGHT_PIXELS"
		String filename = path.toString();
		String sizeinfo = ";" + String.valueOf(width) + ";" + String.valueOf(height);
		int lastSlash = filename.lastIndexOf("/");
		if( lastSlash > 0 && lastSlash+1 < filename.length()){
			key.set(filename.substring(lastSlash+1) + sizeinfo);
		} else {
			key.set(path.toString() + sizeinfo);
		}
		
		//place the data from BufferedImage into a BytesWritable
		float progressIncrement = 1.0f / pixels;
		byte[] rgb;
		for (int x = 0; x < width; x++){
			for(int y = 0; y < height; y++){
				rgb = getPixelData(img.getRGB(x, y));
				for (int k = 0; k < rgb.length; k++){
					values[x + y*width + k*pixels] = rgb[k];
					counter++;
				}
				progress+=progressIncrement;
			}
		}
		
		value = new BytesWritable(values);
		
//		System.out.println("READ IMAGE: " + key.toString() + " PIXELS: " + (counter/3));
		
		fsin.close();
		
	}
	
	private static byte[] getPixelData(int argb) {
		int rgb[] = new int[]{
				//figure this out
				(argb >> 16) & 0xff,
				(argb >> 8) & 0xff,
				(argb ) & 0xff
		};
//		System.out.println((byte) rgb[0] + " " + (byte) rgb[1] + " " + (byte) rgb[2]);
		return new byte[]{ (byte) rgb[0] , (byte) rgb[1], (byte) rgb[2]};
	}
	
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//		System.out.println("initialize");
	}

	@Override
	public Text createKey() {
		return key;
	}

	@Override
	public BytesWritable createValue() {
		return value;
	}

	@Override
	public long getPos() throws IOException {
		return counter;
	}

	@Override
	public float getProgress() throws IOException {
		return progress;
	}

	@Override
	public boolean next(Text arg0, BytesWritable arg1) throws IOException {
		if(!read){
			read = true;
			return true;
		}
		return false;
	}

	@Override
	public void close() throws IOException {
		//tumbleweed
	}
	
}