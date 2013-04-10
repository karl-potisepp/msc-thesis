package ee.ut.sci.potisepp;

import ij.ImagePlus;
import ij.process.ColorProcessor;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

public class ImageData implements Writable {
	
	//image data	
	private float pixels[][];
	private float max;
	private float min;
	public int nx;
	public int ny;
	public int nz;
	public int nc; //no. of channels
	
	//metadata
	TreeMap <String, String> metadata_map;
	public int n_metadata;
	
	public ImageData(ImagePlus imp, TreeMap<String, String> metadata_map){
		if (imp.getType() == ImagePlus.COLOR_RGB)
			this.pixels = convertColor(imp);
			
		else
			this.pixels = convertGrayscale(imp);
		nx = imp.getWidth();
		ny = imp.getHeight();
		nz = imp.getStackSize();
		
		this.metadata_map = new TreeMap<String,String>(metadata_map);
		this.n_metadata = metadata_map.size();
	}
	
	/**
	 * Return the minimum value of the input image.
	 */	
	public float getMinimum() {
		return min;
	}
	
	/**
	 * Return the maximum value of the input image.
	 */	
	public float getMaximum() {
		return max;
	}
	
	/**
	 * 
	 */
	protected float[] getPixels(int channel) {
		return pixels[channel];
	}
	
	protected TreeMap<String, String> getMetadata(){
		return metadata_map;
	}
	
	/**
	 * Convert a grayscale image (ImagePlus) into a 1D float array.
	 */
	private float[][] convertGrayscale(ImagePlus imp) {
		int nx = imp.getWidth();
		int ny = imp.getHeight();
		int nz = imp.getStackSize();
		int nxy = nx*ny;
		this.nc = 1;
		int size = nx * ny * nz;
		min = Float.MAX_VALUE;
		max = -Float.MAX_VALUE;
		int index;
		float in[][] = new float[1][size];
		for(int z=0; z<nz; z++) {
			ImageProcessor ip = imp.getImageStack().getProcessor(z+1);
			if (ip instanceof FloatProcessor) {
				for(int k=0; k<nxy; k++) {
					index = k+z*nxy;
					in[0][index] = (float)ip.getf(k);
					if (in[0][index] < min)
						min = in[0][index];
					if (in[0][k+z*nxy] > max)
						max = in[0][index];
				}
			}
			else {
				for(int k=0; k<nxy; k++) {
					index = k+z*nxy;
					in[0][index] = (float)ip.get(k);
					if (in[0][index] < min)
						min = in[0][index];
					if (in[0][index] > max)
						max = in[0][index];
				}
			}
		}	
		return in;
	}
	
	/**
	 * Convert a color image (ImagePlus) into three 1D float arrays, one
	 * per color channel.
	 */
	private float[][] convertColor(ImagePlus imp) {
		int nx = imp.getWidth();
		int ny = imp.getHeight();
		int nz = imp.getStackSize();
		int size = nx * ny * nz;
		this.nc = 3;
		float in[][] = new float[3][size];
		min = Float.MAX_VALUE;
		max = -Float.MAX_VALUE;
		for(int z=0; z<nz; z++) {
			ColorProcessor cp = (ColorProcessor)imp.getStack().getProcessor(z+1);
			int colors[] = new int[3];
			int index = 0;
			for(int x=0; x<nx; x++)
			for(int y=0; y<ny; y++) {
				index = x+y*nx;
				cp.getPixel(x, y, colors);
				for (int c=0; c<3; c++) {
					in[c][index] = colors[c];
					if (in[c][index] < min)
						min = in[c][index];
					if (in[c][index] > max)
						max = in[c][index];
				}
			}
		}
		
		return in;
	}

	//functions for MapReduce

	@Override
	public void write(DataOutput out) throws IOException {
		
		//TODO test this function
		
		//first write size of metadata
		out.writeInt(n_metadata);
		//then write all the metadata key-value pairs
		Iterator<String> keys_it = metadata_map.keySet().iterator();
		
		String cur_key;
		while(keys_it.hasNext()){
			cur_key = keys_it.next();
			//write key
			out.writeUTF(cur_key);
			//write value
			out.writeUTF(metadata_map.get(cur_key));
		}
		
		//finally, write image pixel data
		int size = nx * ny * nz;
		for (int c = 0; c < nc; c++){
			for (int index = 0; index < size; index++){
				out.writeFloat(pixels[c][index]);
			}
		}		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		//TODO test this function
		
		//first read size of metadata
		this.n_metadata = in.readInt();
		//read all metadata key-value pairs
		for(int i = 0; i < this.n_metadata; i++){
			metadata_map.put(in.readUTF(), in.readUTF());
		}
		
		int size = nx * ny * nz;
		for (int c = 0; c < nc; c++){
			for (int index = 0; index < size; index++){
				pixels[c][index] = in.readFloat();
			}
		}		
	}

}
