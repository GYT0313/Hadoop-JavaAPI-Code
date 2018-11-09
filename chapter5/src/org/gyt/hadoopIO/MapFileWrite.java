package org.gyt.hadoopIO;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

/**
 * @FileName: MapFileWrite.java
 * @Package: org.gyt.io
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2018年11月8日 下午6:48:41
 */

public class MapFileWrite {
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
		URI uri = new URI("hdfs://master:9000/data/number.map");
		FileSystem fs = FileSystem.get(uri, conf);
		IntWritable key = new IntWritable();
		Text value = new Text();
		MapFile.Writer writer = null;
		try {
			writer = new MapFile.Writer(conf, fs, uri.toString(), key.getClass(), value.getClass());
			for (int i=0; i < 1024; i++) {
				key.set(i+1);
				value.set(DATA[i % DATA.length]);
				writer.append(key, value);
			}
		} finally {
			// TODO: handle finally clause
			IOUtils.closeStream(writer);
		}
	}
}





