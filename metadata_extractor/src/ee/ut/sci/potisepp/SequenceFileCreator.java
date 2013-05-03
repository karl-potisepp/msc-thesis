package ee.ut.sci.potisepp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileCreator {
	
	public static ArrayList<String> allPaths;
	
	public static void recursive_walk(File dir) {
 
        File listFile[] = dir.listFiles();
        
        if (listFile != null) {
            for (int i=0; i<listFile.length; i++) {
                if (listFile[i].isDirectory()) {
                	recursive_walk(listFile[i]);
                } else {
                    allPaths.add(listFile[i].getPath());
//                	System.out.println(listFile[i].getPath());                    
                }
            }
        }
    }
	
	public static void main(String[] args) throws IOException
	{
		
		if(args.length != 3){
			System.out.println("Usage: SequenceFileCreator INPUT_DIR OUTPUT_DIR FILES_PER_CHUNK");
		}
		
		String inPath = args[0];
//		inPath = "/home/karl/kool/msc_thesis/git/object_recognition_test";
		String outPath = args[1];
		int filesPerChunk = Integer.valueOf(args[2]);
		filesPerChunk = 250;
		
		allPaths = new ArrayList<String>();
		recursive_walk(new File(inPath));
		
		Iterator<String> it = allPaths.iterator();
		
		Configuration conf = new Configuration();
    	FileSystem fs = FileSystem.get(conf);
		
		String tmp;
		int c = 1, num = 1;
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(outPath+"/"+num+".seq"), 
				Text.class, BytesWritable.class, 
				SequenceFile.CompressionType.NONE);
		num++;
		byte[] tmpbytes;
		while(it.hasNext()){
			
			if(c > 0 && (c % filesPerChunk) == 0){
				writer.close();
				writer = SequenceFile.createWriter(fs, conf, new Path(outPath+"/"+num+".seq"), 
						Text.class, BytesWritable.class, 
						SequenceFile.CompressionType.NONE);
				
				num++;
			}
			
			tmp = it.next();
			
			if(tmp.toLowerCase().endsWith("jpg") || tmp.toLowerCase().endsWith("jpeg")){  
//					|| tmp.toLowerCase().endsWith("png")){
				
				tmpbytes = FileUtils.readFileToByteArray(new File(tmp));
				writer.append( new Text(tmp), new BytesWritable(tmpbytes));
//				System.out.println(tmp);
//				System.out.println(c+1);
											
				c++;
				System.out.println("seqfile " + (num-1) + " file " + (c % filesPerChunk) + " : " + tmp);
			}
					
//			if(c == 10){
//				break;
//			}			
		}
		writer.close();
		
	}

}
