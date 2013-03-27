package ee.ut.sci.potisepp;

import ij.ImagePlus;

import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class MyImageReader implements RecordReader<Text, Data>{
	
	private Text key = new Text();
	private Data value;
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
		
		int bpp = img.getColorModel().getPixelSize();
		
		ImagePlus imp = new ImagePlus("", img);
		img = null;
		value = new Data(imp);
		imp = null;
		
		//set key to the form of "FILENAME.EXTENSION;WIDTH_PIXELS;HEIGHT_PIXELS;NB_CHANNELS
		String filename = path.toString();
		String sizeinfo = "";
		sizeinfo += ";" + String.valueOf(value.nx);
		sizeinfo += ";" + String.valueOf(value.ny);
		sizeinfo += ";" + String.valueOf(value.nc);
		sizeinfo += ";" + String.valueOf(bpp);
		
		int lastSlash = filename.lastIndexOf("/");
		if( lastSlash > 0 && lastSlash+1 < filename.length()){
			key.set(filename.substring(lastSlash+1) + sizeinfo);
		} else {
			key.set(path.toString() + sizeinfo);
		}
		
		
		fsin.close();
		
	}
	
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//		System.out.println("initialize");
	}

	@Override
	public Text createKey() {
		return key;
	}

	@Override
	public Data createValue() {
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
	public boolean next(Text arg0, Data arg1) throws IOException {
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