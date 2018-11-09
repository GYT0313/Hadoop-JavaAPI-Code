package org.gyt.hadoopIO;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * @FileName: SequenceFileRead.java
 * @Package: org.gyt.io
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2018年11月8日 下午4:35:51
 */

public class SequenceFileRead {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://master:9000/data/number.seq");
		FileSystem fs = FileSystem.get(uri, conf);
		Path path = new Path(uri);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key, value)) {
				String syncSeen = reader.syncSeen() ? "*":"";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition();
			}
		} finally {
			// TODO: handle finally clause
			IOUtils.closeStream(reader);
		}
	}
}





