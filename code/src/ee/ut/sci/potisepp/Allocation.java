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

/**
 * This class is in charge of allocate the memory to store all the intermediate
 * results of the Bilateral Filter.
 */

public class Allocation implements Runnable {

	private float[][] array;
	private int nchannels;
	private int size;
	
	protected Allocation(int nchannels, int size) {
		this.nchannels = nchannels;
		this.size = size;
	}
	
	public void run() {
		array = new float[nchannels][size];
	}
	
	protected float[][] getArray() {
		return array;
	}

	protected float[] getChannel(int c) {
		return array[c];
	}
	
	protected int getNumberChannels() {
		return nchannels;
	}

}

