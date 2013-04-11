package ee.ut.sci.potisepp;

import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.PNG_Writer;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.FloatProcessor;
import ij.process.ShortProcessor;


public class SequentialBF {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if(args.length != 6){
			System.out.println("Required arguments: INFILE OUTFILE sigRange sigmaX sigmaY sigmaZ");
			System.exit(0);
		}
		
		String infile = args[0];
		String outfile = args[1];
		float sigRange = Float.parseFloat(args[2]);
		float sigmaX = Float.parseFloat(args[3]);
		float sigmaY = Float.parseFloat(args[4]);
		float sigmaZ = Float.parseFloat(args[5]);
		
		//infile = "../../../big_img/lenna1.png";
		//outfile = "../../../big_img/lenna1_bf.png";
		
		System.out.println("infile: " + infile);
		System.out.println("outfile: " + outfile);
		System.out.println("sigRange: " + sigRange);
		System.out.println("sigmaX: " + sigmaX);
		System.out.println("sigmaY: " + sigmaY);
		System.out.println("sigmaZ: " + sigmaZ);
		
		Filter filter = new Filter(sigRange, sigmaX, sigmaY, sigmaZ);
		
		ImagePlus input = new ImagePlus(infile);
		
		Data data = new Data(input);
				
		filter.setMultithread(true);
		filter.selectChannels(0, 0.0);
		int order[] = filter.computeOrder(data);
		int n = order[3]-order[2];
		if (n > 500)
			return;
		
		// --------------------------------------------------------------------------
		// Convert and process the input image
		// --------------------------------------------------------------------------
		int nx = input.getWidth();
		int ny = input.getHeight();
		int nz = input.getStackSize();
		int nxy = nx*ny;

		ImageStack stack = new ImageStack(nx, ny);
		
		long chrono = System.currentTimeMillis();
		
		if (input.getType() == ImagePlus.COLOR_RGB) {
			int rgb[][] = new int[nz][nxy];
			for(int c=0; c<3; c++) {
				float out[] = filter.execute(data, c);
				int shift = 8*(2-c);
				for(int z=0; z<nz; z++)
				for(int k=0; k<nxy; k++) 
					rgb[z][k] += (byte)(out[z*nxy+k]) << shift;
			}
			for(int z=0; z<nz; z++) {
				ColorProcessor cp = new ColorProcessor(nx, ny);
				cp.setPixels(rgb[z]);
				stack.addSlice("", cp);
			}
		}
		else {
			float out[] = filter.execute(data, 0);
			if (input.getType() == ImagePlus.GRAY8) {
				for(int z=0; z<nz; z++) {
					byte gray[] = new byte[nxy];
					for(int x=0; x<nxy; x++)
						gray[x] = (byte)out[x + nxy*z];
					stack.addSlice("", new ByteProcessor(nx, ny, gray, null));
				}
			}
			else if (input.getType() == ImagePlus.GRAY16) {
				for(int z=0; z<nz; z++) {
					short gray[] = new short[nxy];
					for(int x=0; x<nxy; x++)
						gray[x] = (short)out[x + nxy*z];
					stack.addSlice("", new ShortProcessor(nx, ny, gray, null));
				}
			}
			else if (input.getType() == ImagePlus.GRAY32) {
				for(int z=0; z<nz; z++) {
					float gray[] = new float[nxy];
					for(int x=0; x<nxy; x++)
						gray[x] = out[x + nxy*z];
					stack.addSlice("", new FloatProcessor(nx, ny, gray, null));
				}
			}
		}
		
		System.out.println("Time (ms): " + (System.currentTimeMillis()-chrono) );
				
		if (stack.getSize() == input.getStackSize()) {
			ImagePlus impout = new ImagePlus("BFI on " + input.getTitle() + " (" + sigmaX + "-" + sigRange + ") ", stack);

			PNG_Writer writer = new PNG_Writer();
			
			try {
				writer.writeImage(impout, outfile, 0);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}		

	}

}
