package org.gyt.hadoopIO;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * @FileName: MapFileRead.java
 * @Package: org.gyt.io
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2018年11月8日 下午7:01:15
 */

public class MapFileRead {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws URISyntaxException, IOException {
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://master:9000/data/number.map");
		FileSystem fs = FileSystem.get(uri, conf);
		MapFile.Reader reader = null;
		try {
			reader = new MapFile.Reader(fs, uri.toString(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			reader.get(new IntWritable(1000), value);
			System.out.println("key=1000, value=" +value.toString());
			
			reader.getClosest(new IntWritable(1025), value, true);
			System.out.println("key=1025, value=" + value.toString());
			
			// 循环输出
			for (int i = 0; i<1024; i++) {
				reader.get(new IntWritable(i+1), value);
				System.out.println("key=" + (i+1) + ", value=" + value);
			}
			
		} finally {
			// TODO: handle finally clause
			IOUtils.closeStream(reader);
		}
	}
	
	
}





