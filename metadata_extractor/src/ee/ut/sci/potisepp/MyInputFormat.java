package ee.ut.sci.potisepp;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class MyInputFormat extends FileInputFormat<Text, Data>{

	@Override
	public RecordReader<Text, Data> getRecordReader(InputSplit arg0, JobConf arg1,
			Reporter arg2) throws IOException {
		return new MyImageReader(arg1, arg0);
	}

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename){
		return false;
	}		
	
}

