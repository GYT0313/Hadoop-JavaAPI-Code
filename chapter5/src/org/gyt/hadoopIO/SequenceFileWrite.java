package org.gyt.hadoopIO;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


/**
 * @author: Gu Yongtao
 * @Description: 
 * @date: 2018年11月8日 上午11:27:49
 * @Filename: SequenceFileWrite.java
 */

public class SequenceFileWrite {
	private static final String[] DATA = {
			"One, tow, buckle",
			"Thre, four, shut",
			"Five, six, pick",
			"Seven, eight, lay",
			"Nine, ten, a"
	};

	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://master:9000/data/num.seq/data");
		FileSystem fs = FileSystem.get(uri, conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			// 
			System.out.printf("[%s]\n", writer.getLength());
			for (int i = 0; i < 500; i++) {
				key.set(500-i);
				value.set(DATA[i % DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
				writer.append(key, value);
			}
			
			
		}
		finally {
			IOUtils.closeStream(writer);
		}
		
	}
}





