package ee.ut.sci.potisepp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FloatArrayWritable implements Writable{
	
	private float pixels[][];
	private float max;
	private float min;
	public int nx;
	public int ny;
	public int nz;
	
	public FloatArrayWritable(int nx, int ny, int nz){
		this.nx = nx;
		this.ny = ny;
		this.nz = nz;
		this.pixels = new float[nz][nx*ny];
	}
	
	public FloatArrayWritable(int nx, int ny, int nz, float min, float max, float[][] pixels){
		this.nx = nx;
		this.ny = ny;
		this.nz = nz;
		this.min = min;
		this.max = max;
		this.pixels = pixels;
	}
		
	public float getMinimum() {
		return min;
	}
	
	public float getMaximum() {
		return max;
	}
	
	protected float[] getPixels(int channel) {
		return pixels[channel];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int index;
		for (int z = 0; z < nz; z++){
			for (int x = 0; x < nx; x++){
				for (int y = 0; y < ny; y++){
					index = x+y*nx;
					out.writeFloat(pixels[z][index]);
				}
			}
		}		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int index;
		for (int z = 0; z < nz; z++){
			for (int x = 0; x < nx; x++){
				for (int y = 0; y < ny; y++){
					index = x+y*nx;
					pixels[z][index] = in.readFloat();
				}
			}
		}
	}
		
}