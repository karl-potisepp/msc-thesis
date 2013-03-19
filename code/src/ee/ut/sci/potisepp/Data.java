//=====================================================================================
//
// Project: 
// Constant-time bilateral filter
// 
// Authors: 
// Daniel Sage, Kunal N. Chaudhury
// Biomedical Imaging Group (BIG)
// Ecole Polytechnique Federale de Lausanne (EPFL)
// Lausanne, Switzerland
//
// Date:
// 4 February 2011
//
// Reference:
// Kunal N. Chaudhury, Daniel Sage, and Michael Unser,
// Fast O(1) bilateral filtering using trigonometric range kernels
// IEEE Transactions on Image Processing, submitted Dec. 2010
//
// Conditions of use:
// You'll be free to use this software for research purposes, but you
// should not redistribute it without our consent. In addition, we 
// expect you to include a citation or acknowledgment whenever 
// you present or publish results that are based on it.
//
//=====================================================================================
package ee.ut.sci.potisepp;

import ij.ImagePlus;
import ij.process.ColorProcessor;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;

/**
 * This class contains the input data.
 */
public class Data {
	
	private float pixels[][];
	private float max;
	private float min;
	public int nx;
	public int ny;
	public int nz;
	
	/**
	* Constructor.
	*/
	public Data(ImagePlus imp) {
		if (imp.getType() == ImagePlus.COLOR_RGB)
			this.pixels = convertGolor(imp);
			
		else
			this.pixels = convertGrayscale(imp);
		nx = imp.getWidth();
		ny = imp.getHeight();
		nz = imp.getStackSize();
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
	
	/**
	 * Convert a grayscale image (ImagePlus) into a 1D float array.
	 */
	private float[][] convertGrayscale(ImagePlus imp) {
		int nx = imp.getWidth();
		int ny = imp.getHeight();
		int nz = imp.getStackSize();
		int nxy = nx*ny;
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
	private float[][] convertGolor(ImagePlus imp) {
		int nx = imp.getWidth();
		int ny = imp.getHeight();
		int nz = imp.getStackSize();
		int size = nx * ny * nz;
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
}
