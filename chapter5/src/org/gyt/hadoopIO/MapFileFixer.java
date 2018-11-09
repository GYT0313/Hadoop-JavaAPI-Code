package org.gyt.hadoopIO;


import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

/**
 * @FileName: MapFileFixer.java
 * @Package: org.gyt.io
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2018年11月8日 下午7:56:52
 */

public class MapFileFixer {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://master:9000/data/num.seq");
		FileSystem fs = FileSystem.get(uri, conf);
		Path map = new Path(uri.toString());
		Path mapData = new Path(map, MapFile.DATA_FILE_NAME);
		// 通过SequenceFile.Reader 获取SequenceFile的key和value类型
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, mapData, conf);
		Class keyClass = reader.getKeyClass();
		Class valueClass = reader.getValueClass();
		reader.close();
		// 使用MapFile.fix 把一个Sequence转换成MapFile
		long entries = MapFile.fix(fs, map, keyClass, valueClass, false, conf);
		System.out.printf("Created MapFile %s with %d entries\n", map, entries);
		
	}
}





