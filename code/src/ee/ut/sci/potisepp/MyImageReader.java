package ee.ut.sci.potisepp;

import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class MyImageReader implements RecordReader<Text, FloatArrayWritable>{
	
	private Text key = new Text();
//	private IntArrayWritable value = new IntArrayWritable();
	private FloatArrayWritable value = new FloatArrayWritable();
//	private BytesWritable value = new BytesWritable();
	private boolean read = false;
			
	int counter = 0;
	private float progress = 0.0f;
	
	BufferedImage img;
	FileSplit split;
	Path path;
	FileSystem fs;
	FSDataInputStream fsin;
	
	public MyImageReader(Configuration jobConf, InputSplit inputSplit) throws IOException {
		
		//we assume here that the image is not split in such a way that it's unreadable :)
		split = (FileSplit) inputSplit;
		path = split.getPath();
		fs = path.getFileSystem(jobConf);						
		FSDataInputStream fsin = fs.open(path);
		img = ImageIO.read(fsin);
		int width = img.getWidth();
		int height = img.getHeight();		
		int channels = img.getColorModel().getNumColorComponents();
		int bpp = img.getColorModel().getPixelSize();
		int pixels = width * height;
		float min = Float.MAX_VALUE, max = Float.MIN_VALUE;
		
//		byte[] values = new byte[width * height * channels];
		FloatWritable[] values = new FloatWritable[width * height * channels];
		
		//place the data from BufferedImage into a BytesWritable
		float progressIncrement = 1.0f / pixels;
		byte[] rgb;
		for (int x = 0; x < width; x++){
			for(int y = 0; y < height; y++){
				rgb = getPixelData(img.getRGB(x, y));
				for (int k = 0; k < rgb.length; k++){
					values[x + y*width + k*pixels] = new FloatWritable(rgb[k]);
					
					if (rgb[k] > max) max = rgb[k];
					if (rgb[k] < min) min = rgb[k];
					
					counter++;
				}
				progress+=progressIncrement;
			}
		}
		
		//set key to the form of "FILENAME.EXTENSION;WIDTH_PIXELS;HEIGHT_PIXELS;NO_CHANNELS;BITS_PER_PIXEL;MIN;MAX"
		String filename = path.toString();
		String sizeinfo = "";
		sizeinfo += ";" + String.valueOf(width);
		sizeinfo += ";" + String.valueOf(height);
		sizeinfo += ";" + String.valueOf(channels);
		sizeinfo += ";" + String.valueOf(bpp);
		sizeinfo += ";" + String.valueOf(min);
		sizeinfo += ";" + String.valueOf(max);
		int lastSlash = filename.lastIndexOf("/");
		if( lastSlash > 0 && lastSlash+1 < filename.length()){
			key.set(filename.substring(lastSlash+1) + sizeinfo);
		} else {
			key.set(path.toString() + sizeinfo);
		}
		
		value = new FloatArrayWritable(values);
		
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
	public FloatArrayWritable createValue() {
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
	public boolean next(Text arg0, FloatArrayWritable arg1) throws IOException {
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