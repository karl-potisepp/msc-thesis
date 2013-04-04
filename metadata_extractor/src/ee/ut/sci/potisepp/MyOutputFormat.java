package ee.ut.sci.potisepp;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class MyOutputFormat extends FileOutputFormat<Text, Data>{

	@Override
	public RecordWriter<Text, Data> getRecordWriter(
			FileSystem fs, JobConf conf, String name, Progressable progress)
			throws IOException {
		return new MyImageWriter(fs, conf, name, progress);
	}
}