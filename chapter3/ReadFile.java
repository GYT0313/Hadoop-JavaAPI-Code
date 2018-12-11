package org.gyt.pack01;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @FileName: ReadFile.java
 * @Package: org.gyt.pack01
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2018年11月1日 下午2:27:16
 */

public class ReadFile {
	public static void main(String[] args) throws IOException, URISyntaxException {
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://slave2:9000"), new Configuration());
		Path path = new Path("/File.txt");
		FSDataInputStream fDataInputStream = fileSystem.open(path);
		// 将字节流转换为字符流
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fDataInputStream));
		// 读取
		String content = bufferedReader.readLine();
		while (content!=null) {
			System.out.println(content);
			content = bufferedReader.readLine();
		}
		// 关闭
		bufferedReader.close();
		fDataInputStream.close();
		fileSystem.close();
	}
}
