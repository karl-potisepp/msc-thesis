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

import ij.IJ;
//import ij.ImagePlus;
//import ij.ImageStack;
//import ij.process.FloatProcessor;

import java.text.DecimalFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//import additionaluserinterface.WalkBar;

/**
 * This class contains the processing of the Bilateral Filter.
 * 
 * @author sage
 *
 */
public class Filter {
	
	static public int ITERATION_NONE = 0;
	static public int ITERATION_RANGE = 1;
	static public int ITERATION_SPACE = 2;
	static public int ITERATION_OVERALL = 3;
	
	static public int SELECTION_BEST = 0;
	static public int SELECTION_ALL = 1;
	static public int SELECTION_BYTRUNCATION = 2;
	static public int SELECTION_BYTOLERANCE = 3;
	
	private int nbIteration=1;
	private int modeIteration=ITERATION_NONE;
	private double valueSelectionChannels = 0.0;
	private int methodSelectionChannels = SELECTION_BEST;
	private float sigmaX, sigmaY, sigmaZ;
	private float sigmaR;
	
	private double chrono;				// Use to store the elapsed time
	private boolean multithread = true;
	private boolean log = false;
	private float T;					// Dynamic range of the input image
	
	/**
	* Constructor.
	*/
	public Filter(float sigmaR, float sigmaX, float sigmaY, float sigmaZ) {
		this.sigmaR = sigmaR;
		this.sigmaX = sigmaX;
		this.sigmaY = sigmaY;
		this.sigmaZ = sigmaZ;
	}
	
	/**
	 * Specifies the log status.
	 */
	public void setLog(boolean log) {
		this.log = log;
	}

	/**
	 * Specifies the multithread status.
	 */
	public void setMultithread(boolean multithread) {
		this.multithread = multithread;
	}

	/**
	 * Specifies the iterative scheme.
	 */
	public void setIterativeScheme(int nbIteration, int modeIteration) {
		this.nbIteration = (modeIteration == ITERATION_NONE ? 1 : nbIteration);
		this.modeIteration = modeIteration;
	}
	
	/**
	 * Specifies the select of the channels.
	 */
	public void selectChannels(int methodSelectionChannels, double valueSelectionChannels) {
		this.methodSelectionChannels = methodSelectionChannels;
		this.valueSelectionChannels = valueSelectionChannels;
	}
		
	/**
	 * Determines the order of the filter. The order is function of
	 * the input dynamic range (T) and the size of the filter
	 * in the range domain (sigmaR).
	 * 
	 * To avoid too many channels only a subset of the channels can be 
	 * optionally applied. This selection of the channel is based on the user 
	 * choice: 
	 * 1) Best: only the channels corresponding to a weight which is
	 * 0.001 larger than the larger one is take in account
	 * 2) All: all channels
	 * 3) By truncate: 
	 * 4) By tolerance:  
	 */	
	public int[] computeOrder(Data data) {
		
		float min = data.getMinimum();
		float max = data.getMaximum();
		
		T = max - min;
		int N = 3;
		double gamma = 0.5 * Math.PI / T;
		double rho = gamma * sigmaR;
		if (sigmaR > 1.0/gamma/gamma)
			N = 3;
		else
			N = Math.max(3, (int)Math.ceil(1.0/rho/rho));
		
		int nchannels = N+1;
		int cmin = 0;
		int cmax = nchannels;
		int trunc = 0;
		if (methodSelectionChannels == SELECTION_BEST) {
			double coef[] = computeCoefficient(0, nchannels, N);
			trunc = (coef.length + 1)/2;
			double maxC = coef[trunc];
			while (trunc >= 0 && maxC/coef[trunc] < 1000.0) {
				trunc--;
			}
		}
		else if (methodSelectionChannels == SELECTION_ALL) {
			trunc = 0;
		}
		else if (methodSelectionChannels == SELECTION_BYTRUNCATION) {
			double truncate = Math.max(0, Math.min(100, valueSelectionChannels));
			trunc = (int)Math.round(nchannels * truncate/200.0);
		}
		else if (methodSelectionChannels == SELECTION_BYTOLERANCE) {
			double coef[] = computeCoefficient(0, nchannels, N);
			double toleranceMax = valueSelectionChannels/100.0; 
			trunc = 0;
			double tolerance = 0.0; 
			while(trunc < coef.length/2 && tolerance < toleranceMax) {
				tolerance += 2*coef[trunc];
				trunc++;
			}
		}
		
		cmin = trunc;
		cmax = nchannels-trunc;
		if (cmax <= cmin)
			cmax = cmin+1;
		return new int[] {N, nchannels, cmin, cmax};
	}
	
	/**
	 * Processes the image (in).
	 */
//	public float[] execute(WalkBar walk, Data data, int channel) {
	public float[] execute(Data data, int channel) {
		chrono = System.currentTimeMillis();
		float[] in = data.getPixels(channel);
		
		int nx = data.nx;
		int ny = data.ny;
		int nz = data.nz;
	
		int order[] = computeOrder(data);
		int N = order[0];
		int cmin = order[2];
		int cmax = order[3];
		
		int size = nx*ny*nz;
		
		if (log) {
			IJ.log("Settings N:" + N + " T:" + T);
		}
		
		// --------------------------------------------------------------------------
		// Initialization
		// --------------------------------------------------------------------------
		float factor[] = computeFactorArgument(cmin, cmax, N);
		double coef[] = computeCoefficient(cmin, cmax, N);
		
		// When the number of channels is even (symmetric) every has the same weight
		// When the number of channels is odd, all the channels has the same weight, except the
		// one which count all 1/2 times.
		if (factor.length % 2 != 0)
			coef[coef.length-1] *= 0.5;
		
		int halfChannels = (factor.length+1)/2;
			
		if (log) {
			DecimalFormat formatE = new DecimalFormat("0.00E00");
			for(int k=0; k<halfChannels; k++)
				IJ.log("k: "  + (k+cmin) + " >" + formatE.format(coef[k]) + " * cos(" + factor[k] + " x theta)");
		}
		
//		if (walk != null)
//			walk.progress("Starting ...", 10);
		
		if (log)
			IJ.log("Memory allocation " + (6*halfChannels*size)/1024 + " Kb");		

		float gamma = 0.5f * (float)Math.PI / T;
		float rho = gamma * sigmaR;
		float cst = gamma / rho / (float)Math.sqrt(N);
		
		// --------------------------------------------------------------------------
		// Memory allocation
		// --------------------------------------------------------------------------
		Allocation cos  = new Allocation(halfChannels, size);
		Allocation gcos = new Allocation(halfChannels, size);
		Allocation fcos = new Allocation(halfChannels, size);
		Allocation sin  = new Allocation(halfChannels, size);
		Allocation gsin = new Allocation(halfChannels, size);
		Allocation fsin = new Allocation(halfChannels, size);
		if (multithread) {
			ExecutorService executor = Executors.newFixedThreadPool(2);
			executor.execute(cos);
			executor.execute(gcos);
			executor.execute(fcos);
			executor.execute(sin);
			executor.execute(gsin);
			executor.execute(fsin);
			executor.shutdown();
			while (!executor.isTerminated()) {}
		}
		else {
			cos.run();
			gcos.run();
			fcos.run();
			sin.run();
			gsin.run();
			fsin.run();
		}
		
		float output[] = new float[size];
		for(int x=0; x<size; x++)
			output[x] = in[x];
		
		for(int iter=0; iter<nbIteration; iter++) {
//			if (walk != null)
//				walk.progress("Starting iteration " + (iter+1), 10);
			float inSpace[] = in;
			float inRange[] = in;
			if (modeIteration == ITERATION_SPACE)
				inSpace = output;
			if (modeIteration == ITERATION_RANGE)
				inRange = output;
			if (modeIteration == ITERATION_OVERALL) {
				inRange = output;
				inSpace  = output;
			}

			Initialization initCos = new Initialization(Initialization.COS, cos, gcos, fcos, cst, factor, inSpace, inRange);
			Initialization initSin = new Initialization(Initialization.SIN, sin, gsin, fsin, cst, factor, inSpace, inRange);
		
			if (multithread) {
				ExecutorService executor = Executors.newFixedThreadPool(2);
				executor.execute(initCos);
				executor.execute(initSin);
				executor.shutdown();
				while (!executor.isTerminated()) {}
			}
			else {
				initCos.run();
				initSin.run();
			}
			
			log("Initialisation");		
//			if (walk != null)
//				walk.progress("Initialisation", 15);
		
			// --------------------------------------------------------------------------
			// Gaussian
			// --------------------------------------------------------------------------
			int nGaussians = 4*halfChannels;
 			Gaussian gaussians[] = new Gaussian[nGaussians];
			for(int c=0; c<halfChannels; c++) {
				gaussians[4*c  ] = new Gaussian(fcos.getChannel(c), sigmaX, sigmaY, sigmaZ, nx, ny, nz, cos.getChannel(c));
				gaussians[4*c+1] = new Gaussian(gcos.getChannel(c), sigmaX, sigmaY, sigmaZ, nx, ny, nz, cos.getChannel(c));
				gaussians[4*c+2] = new Gaussian(fsin.getChannel(c), sigmaX, sigmaY, sigmaZ, nx, ny, nz, sin.getChannel(c));
				gaussians[4*c+3] = new Gaussian(gsin.getChannel(c), sigmaX, sigmaY, sigmaZ, nx, ny, nz, sin.getChannel(c));
			}
			
			if (multithread) {
				ExecutorService executor = Executors.newFixedThreadPool(nGaussians);
				for(int i=0; i<nGaussians; i++){
					executor.execute(gaussians[i]);
				}
				executor.shutdown();
				while (!executor.isTerminated()) {}
			}
			else {
				for(int i=0; i<nGaussians; i++)
					gaussians[i].run();
			}
			log("Gaussian convolutions");		
//			if (walk != null)
//				walk.progress("Gaussians x" + nGaussians, 95);
			
			float arr_fcos[][] = fcos.getArray();
			float arr_gcos[][] = gcos.getArray();
			float arr_fsin[][] = fsin.getArray();
			float arr_gsin[][] = gsin.getArray();
			float value;
			double dem, num;
			float min = data.getMinimum();
			float max = data.getMaximum();
			for(int k=0; k<size; k++) {
				num = 0.0f;
				dem = 0.0f;
				for(int c=0; c<halfChannels; c++) {
					num += coef[c] * (arr_fcos[c][k] + arr_fsin[c][k]);
					dem += coef[c] * (arr_gcos[c][k] + arr_gsin[c][k]);
				}
				value = (float)(num/dem);
				if (value < min)
					value = min;
				if (value > max)
					value = max;
				output[k] = (float)value;
			}
			
		}
		
		if (log) {
			int bins = 200;
			float gaussianApproximated[] = new float [bins];
			float gaussianDiscretized[] = new float[bins];
			float quality[] = approximate(data, gaussianApproximated, gaussianDiscretized);
			IJ.log("Dymanic range of the image=[" + cmin + "..." + cmax + "]");
			IJ.log("Selection of channels method: " + methodSelectionChannels + " value=" + valueSelectionChannels);
			IJ.log("Approximation RMSE: " + quality[0]);
			IJ.log("Approximation Undershoot: "+ quality[1]);
			IJ.log("Approximation Attenuation: " + quality[2]);
		}
		
		return output;
	}

	/**
	 * Return 3 indicators of the quality of the approximation of the
	 * gaussian function discretized over number of bins elements.
	 * 1. RMSE (0 is perfect)
	 * 2. Undershoot: most negative value (0 is perfect)
	 * 3. Lost of gain: 1 - approx(0) (0 is perfect)
	 */
	public float[] approximate(Data data, float gaussian[], float approx[]) {
		int bins = gaussian.length;
		int order[] = computeOrder(data);
		int N = order[0];
		int cmin = order[2];
		int cmax = order[3];
		float factor[] = computeFactorArgument(cmin, cmax, N);
		double coef[] = computeCoefficient(cmin, cmax, N);
		
		// Compute approximated gaussian using raised cosine
		float gamma = 0.5f * (float)Math.PI / T;
		float rho = gamma * sigmaR;
		float cst = gamma / rho / (float)Math.sqrt(N);
		for(int k=0; k<bins; k++) {
			float value = ((float)k/bins)*(2*T) - T;
			for(int c=0; c<factor.length; c++)
				approx[k] += coef[c] * Math.cos(factor[c] * cst * value);
		}
		
		// Compute discretized gaussian
		for(int k=0; k<bins; k++) {
			float value = ((float)k/bins)*(2*T) - T;
			gaussian[k] = (float)Math.exp(-(value)*(value)/sigmaR/sigmaR/2.0f);
		}
		
		// Compute quality indicators
		if (gaussian.length<1)
			return new float[] {Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY};
			
		float rmse = 0.0f;
		float undershoot = Float.MAX_VALUE;
		float gain = -Float.MAX_VALUE;
		for(int k=0; k<bins; k++) {
			rmse += (approx[k]-gaussian[k])*(approx[k]-gaussian[k]);
			if (approx[k] < undershoot)
				undershoot = approx[k];
			if (approx[k] > gain)
				gain = approx[k];
		}	
		rmse = (float)Math.sqrt(rmse/bins);
		return new float[] {rmse, undershoot, 1.0f-gain};
	}

	/**
	 * Computes the factor of the sin and cos for each channels.
	 */
	private float[] computeFactorArgument(int cmin, int cmax, int N) {
		float factor[] = new float[cmax-cmin];
		for(int n=cmin; n<cmax; n++)
			factor[n-cmin] = N-2*n;
		return factor;
	}
	
	/**
	 * Computes the coefficients of the sin and cos for each channels.
	 */
	private double[] computeCoefficient(int cmin, int cmax, int N) {
		double coef[] = new double[cmax-cmin];
		for(int n=cmin; n<cmax; n++)
			coef[n-cmin] = binomial(N, n);
		return coef;
	}
	
	/** 
	 * Compute binomial coefficient.
	 */
	private double binomial(int n, int k) {
		double norm = 1.0 / (double)Math.pow(2, n);
		if (norm < Double.MIN_VALUE) 
			return Double.MIN_VALUE;
		if (k == 0)
			return norm;
		if (k == n)
			return norm;
		double result = 1.0;
		for(int t=0; t<k; t++) 
			result = result * (double)(n-t)/(double)(k-t);
		return result*norm;
	}
	
	/**
	* Print a log message + elapsed time.
	*/
	private void log(String message) {	
		 if (!log)
			return;
		IJ.log(message + " : " + (System.currentTimeMillis() - chrono) + " ms");		
		chrono = System.currentTimeMillis();
	}
	
	/** 
	 * Show a 1D float array as a 32-bit image.
	 */
//	private void show(double[] pix, String title, int nx, int ny) {
//		float f[] = new float[nx*ny];
//		for (int k=0; k<ny*nx; k++)
//			f[k] = (float)pix[k];
//		FloatProcessor fp = new FloatProcessor(nx, ny, f, null);
//		(new ImagePlus(title, fp)).show();
//	}
	
	/** 
	 * Show a 1D float array as a stack of 32-bit images.
	 */
//	private void show(double[] pix, String title, int nx, int ny, int nz) {
//		
//		ImageStack stack = new ImageStack(nx, ny);
//		for(int z=0; z<nz; z++) {
//			float f[] = new float[nx*ny];
//			for (int k=0; k<ny*nx; k++)
//				f[k] = (float)pix[z*nx*ny+k];
//			stack.addSlice("", new FloatProcessor(nx, ny, f, null));
//			if (z==10)
//				(new ImagePlus("show", new FloatProcessor(nx, ny, f, null))).show();
//		}
//		(new ImagePlus(title, stack)).show();
//	}
}
