package ee.ut.sci.potisepp;


import ij.ImagePlus;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;

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

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;

public class MyImageReader implements RecordReader<Text, ImageData>{
	
	private Text key = new Text();
	private ImageData value;
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
		BufferedInputStream bin = new BufferedInputStream(fsin);		
		
		//extract and store all metadata in a hashmap
		//http://drewnoakes.com/code/exif/
		Metadata metadata = null;
		try {
			metadata = ImageMetadataReader.readMetadata(bin, true);
		} catch (ImageProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		TreeMap <String, String> metadata_map = new TreeMap<String, String>();
		HashSet<String> ignore_tags = new HashSet<String>();
		ignore_tags.add("Auto Flash Mode");
		ignore_tags.add("Image Boundary");
		ignore_tags.add("Image Data Size");
		ignore_tags.add("Model");
		ignore_tags.add("Metering Mode");
		ignore_tags.add("Lens");
		ignore_tags.add("CFA Pattern");
		ignore_tags.add("Coded Character Set");
		ignore_tags.add("Image Width");
		ignore_tags.add("F-Number");
		ignore_tags.add("Image Authentication");
		ignore_tags.add("YCbCr Positioning");
		ignore_tags.add("IPTC-NAA Record");
		ignore_tags.add("White Balance Fine");
		ignore_tags.add("Flash Sync Mode");
		ignore_tags.add("Exif Image Height");
		ignore_tags.add("Focal Length");
		ignore_tags.add("Unknown 20");
		ignore_tags.add("Unknown 27");
		ignore_tags.add("VR Info");
		ignore_tags.add("AF Type");
		ignore_tags.add("White Balance Mode");
		ignore_tags.add("Copyright Flag");
		ignore_tags.add("Exif Image Width");
		ignore_tags.add("Scene Capture Type");
		ignore_tags.add("AF Tune");
		ignore_tags.add("AE Bracket Compensation");
		ignore_tags.add("Interoperability Index");
		ignore_tags.add("File Info");
		ignore_tags.add("Lens Stops");
		ignore_tags.add("White Balance");
		ignore_tags.add("Subject Distance Range");
		ignore_tags.add("Aperture Value");
		ignore_tags.add("Orientation");
		ignore_tags.add("Date Created");
		ignore_tags.add("Sub-Sec Time");
		ignore_tags.add("Sub-Sec Time Digitized");
		ignore_tags.add("Exposure Time");
		ignore_tags.add("FlashPix Version");
		ignore_tags.add("Exposure Tuning");
		ignore_tags.add("AF Info 2");
		ignore_tags.add("GPS Version ID");
		ignore_tags.add("Lens Type");
		ignore_tags.add("Software");
		ignore_tags.add("Flash Info");
		ignore_tags.add("Vignette Control");
		ignore_tags.add("ISO");
		ignore_tags.add("Time Created");
		ignore_tags.add("Crop High Speed");
		ignore_tags.add("Interoperability Version");
		ignore_tags.add("Shot Info");
		ignore_tags.add("Shutter Speed Value");
		ignore_tags.add("Application Record Version");
		ignore_tags.add("Program Shift");
		ignore_tags.add("Color Space");
		ignore_tags.add("Firmware Version");
		ignore_tags.add("Components Configuration");
		ignore_tags.add("Focal Length 35");
		ignore_tags.add("Max Aperture Value");
		ignore_tags.add("Exposure Bias Value");
		ignore_tags.add("Picture Control");
		ignore_tags.add("Lens Information");
		ignore_tags.add("Component 1");
		ignore_tags.add("Component 2");
		ignore_tags.add("Component 3");
		ignore_tags.add("Flash Bracket Compensation");
		ignore_tags.add("Lens Data");
		ignore_tags.add("Sub-Sec Time Original");
		ignore_tags.add("ISO Info");
		ignore_tags.add("Flash Exposure Compensation");
		ignore_tags.add("White Balance RB Coefficients");
		ignore_tags.add("Image Height");
		ignore_tags.add("Preview IFD");
		ignore_tags.add("Exposure Difference");
		ignore_tags.add("Gain Control");
		ignore_tags.add("Caption Digest");
		ignore_tags.add("Y Resolution");
		ignore_tags.add("X Resolution");
		ignore_tags.add("World Time");
		ignore_tags.add("Retouch History");
		ignore_tags.add("Resolution Unit");
		ignore_tags.add("Number of Components");
		ignore_tags.add("ISO Speed Ratings");
		ignore_tags.add("High ISO Noise Reductio");
		ignore_tags.add("Exif Version");
		ignore_tags.add("Date/Time Digitized");
		ignore_tags.add("Date/Time Original");
		ignore_tags.add("Color Balance");
		ignore_tags.add("Compressed Bits Per Pixel");
		ignore_tags.add("Compression Type");
		
		for (Directory directory : metadata.getDirectories()) {
		    for (Tag tag : directory.getTags()) {
		    	if( !ignore_tags.contains(tag.getTagName()) ){
		    		metadata_map.put(tag.getTagName(), tag.getDescription());
		    	}		        
		    }
		}
		bin.close();
		fsin.close();
		
		fsin = fs.open(path);
		img = ImageIO.read(fsin);
//		int bpp = img.getColorModel().getPixelSize();
		
		ImagePlus imp = new ImagePlus("", img);
		
		this.value = new ImageData(imp, metadata_map);
		img = null;
		imp = null;
		fsin.close();
		
		//set key to the form of "FILENAME.EXTENSION
		String filename = path.toString();
		String sizeinfo = "";
//		sizeinfo += ";" + String.valueOf(value.nx);
//		sizeinfo += ";" + String.valueOf(value.ny);
//		sizeinfo += ";" + String.valueOf(value.nc);
//		sizeinfo += ";" + String.valueOf(bpp);
		
		int lastSlash = filename.lastIndexOf("/");
		if( lastSlash > 0 && lastSlash+1 < filename.length()){
			key.set(filename.substring(lastSlash+1) + sizeinfo);
		} else {
			key.set(path.toString() + sizeinfo);
		}
		
	}
	
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//		System.out.println("initialize");
	}

	@Override
	public Text createKey() {
		return key;
	}

	@Override
	public ImageData createValue() {
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
	public boolean next(Text arg0, ImageData arg1) throws IOException {
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