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
 * This class is responsible for the initialization procedure of the 
 * sin and cos tables.
 * Thread or directly by calling the run()
 */
public class Initialization implements Runnable {

	public static int SIN = 0;
	public static int COS = 1;
	private int type = SIN;
	
	private Allocation trigo;
	private Allocation gtrigo;
	private Allocation ftrigo;
	private float cst;
	private float[] factor;
	private float[] inSpace;
	private float[] inRange;
	
	public Initialization(int type, Allocation trigo, Allocation gtrigo, Allocation ftrigo, 
			float cst, float[] factor, float[] inSpace, float[] inRange) {
		this.type = type;
		this.trigo = trigo;
		this.gtrigo = gtrigo;
		this.ftrigo = ftrigo;
		this.cst = cst;
		this.factor = factor;
		this.inSpace = inSpace;
		this.inRange = inRange;
	}
	
	public void run() {
		int size = inSpace.length;
		int nchannels = trigo.getNumberChannels();
		for(int c=0; c<nchannels; c++) {
			float[] t = trigo.getChannel(c); 
			float[] g = gtrigo.getChannel(c); 
			float[] f = ftrigo.getChannel(c); 
			float arg;
			if (type == COS) {
				for(int x=0; x<size; x++) {			
					arg = factor[c] * cst * inSpace[x];
					t[x] = (float)Math.cos(arg);	
					g[x] = t[x];	
					f[x] = inRange[x] * t[x];	
				}
			}
			else {
				for(int x=0; x<size; x++) {			
					arg = factor[c] * cst * inSpace[x];
					t[x] =(float) Math.sin(arg);	
					g[x] = t[x];	
					f[x] = inRange[x] * t[x];	
				}
			}
		}
	}
}

