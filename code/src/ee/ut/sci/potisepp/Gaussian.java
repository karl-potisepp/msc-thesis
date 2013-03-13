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
 * Gaussian filter class.
 * Implementation of the Gaussian filter as a cascade of 3 exponential fitlers. 
 * The boundary conditions are mirroring.
 * Thread or directly by calling the run()
 */

public class Gaussian implements Runnable {

	private float signal[];
	private float sigmaX, sigmaY, sigmaZ;
	private int nx;
	private int ny;
	private int nz;
	private int size;
	private float weight[];
	private boolean running = false;
	
	/**
	* Constructor based on the signal.
	*/
	public Gaussian(float signal[], float sigmaX, float sigmaY, float sigmaZ, int nx, int ny, int nz, float weight[]) {
		this.signal = signal;
		this.sigmaX = sigmaX;
		this.sigmaY = sigmaY;
		this.sigmaZ = sigmaZ;
		this.nx = nx;
		this.ny = ny;
		this.nz = nz;
		size = nx*ny*nz;
		this.weight = weight;
	}
	
	/**
	 * 
	 */
	public boolean isRunning() {
		return running;
	}
	
	/**
	* Run method.
	*/
	public void run() {
	
		running = true;
		if (nx > 1 & sigmaX > 0) {
			float row[]  = new float[nx];
			float s2 = sigmaX * sigmaX;
			float pole = 1.0f + (3.0f/s2) - (float)(Math.sqrt(9.0+6.0*s2)/s2);
			for (int y=0; y<ny*nx; y+=nx) {
				System.arraycopy(signal, y, row, 0, nx);
				row = convolveIIR_TriplePole(row, pole);
				System.arraycopy(row, 0, signal, y, nx);
			}
		}
		
		if (ny > 1 & sigmaY > 0) {
			float liney[]  = new float[ny];
			float s2 = sigmaY * sigmaY;
			float pole = 1.0f + (3.0f/s2) - (float)(Math.sqrt(9.0+6.0*s2)/s2);
			for (int z=0; z<nz; z++)
			for (int x=0; x<nx; x++) {
				for(int y=0; y<ny; y++)
					liney[y] = signal[(z*ny+y)*nx+x];
				liney = convolveIIR_TriplePole(liney, pole);
				for(int y=0; y<ny; y++)
					signal[(z*ny+y)*nx+x] = liney[y];
			}
		}
		
		if (nz > 1 & sigmaZ > 0) {
			float line[]  = new float[nz];
			float s2 = sigmaZ * sigmaZ;
			float pole = 1.0f + (3.0f/s2) - (float)(Math.sqrt(9.0+6.0*s2)/s2);
			for (int y=0; y<ny; y++)
			for (int x=0; x<nx; x++) {
				for(int z=0; z<nz; z++)
					line[z] = signal[(z*ny+y)*nx+x];
				line = convolveIIR_TriplePole(line, pole);
				for(int z=0; z<nz; z++)
					signal[(z*ny+y)*nx+x] = line[z];
			}
		}
		
		if (weight != null) {
			for (int k=0; k<size; k++)
				signal[k] *= weight[k];
		}
		running = false;
	}

	/**
	* Convolves with with a Infinite Impulse Response filter (IIR)
	*/
	private float[] convolveIIR_TriplePole(float[] signal, float pole) {
		int l = signal.length;
		
		float lambda = 1.0f;
		float[] output = new float[l];
		for (int k=0; k<3; k++) {
			lambda = lambda * (1.0f - pole) * (1.0f - 1.0f / pole);
		}
		for (int n=0; n<l; n++) {
			output[n] = signal[n] * lambda;
		}
		for (int k=0; k<3; k++) {
			output[0] = getInitialCausalCoefficientMirror(output, pole);
			for (int n=1; n<l ; n++) {
				output[n] = output[n] + pole * output[n - 1];
			}
			output[l-1] = getInitialAntiCausalCoefficientMirror(output, pole);
			for (int n=l-2; 0 <= n; n--) {
				output[n] = pole * (output[n+1] - output[n]);
			}
		}
		return output;
	}

	/**
	* Initial conditions
	*/
	private float getInitialAntiCausalCoefficientMirror(float[] c, float z) {
		return((z * c[c.length-2] + c[c.length-1]) * z / (z * z - 1.0f));
	}

	/**
	* Initial conditions
	*/
	private float getInitialCausalCoefficientMirror(float[] c, float z) {
		float tolerance = 10e-6f;
		float z1 = z;
		float zn = (float)Math.pow(z, c.length - 1);
		float sum = c[0] + zn * c[c.length - 1];
		int horizon = c.length;

		if (tolerance > 0.0f) {
			horizon = 2 + (int)(Math.log(tolerance) / Math.log(Math.abs(z)));
			horizon = (horizon < c.length) ? (horizon) : (c.length);
		}
		zn = zn * zn;
		for (int n=1; n<horizon-1; n++) {
			zn = zn / z;
			sum = sum + (z1 + zn) * c[n];
			z1 = z1 * z;
		}
		return (sum / (1.0f - (float)Math.pow(z, 2 * c.length - 2)));
	}
	
}

