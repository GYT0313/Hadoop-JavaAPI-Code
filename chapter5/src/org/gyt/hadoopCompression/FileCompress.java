package org.gyt.hadoopCompression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

/**
 * @FileName: FileCompress.java
 * @Package: org.gyt.hadoopCompression
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2018年11月9日 上午11:09:16
 */

public class FileCompress {
	public static void main(String[] args) throws IOException {
		// 解压 1.bz2 为 1 -- createInputStream
		String uri = "hdfs://master:9000/data/1.bz2";
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(uri.toString()), conf);
		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath); // 根据文件后缀判断生成何种类型的CompressionCodec
		if (codec == null) {
			System.out.println("No codec found for " + uri);
			System.exit(1); // 异常退出
		}
		// 解压的路径名
		String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			inputStream = codec.createInputStream(fileSystem.open(inputPath)); // 对输入流进行解压
			outputStream = fileSystem.create(new Path(outputUri)); // 创建输出文件，获得输出流
			IOUtils.copyBytes(inputStream, outputStream, conf); // 从输入流到输出流复制, 实现解压
		} finally {
			// TODO: handle finally clause
			IOUtils.closeStream(inputStream);
			IOUtils.closeStream(outputStream);
		}
		
		// 压缩 1 为 1.gz  -- createOutputStream
		CompressionOutputStream compressionOutputStream = null;
		// 压缩
		Path gzPath = new Path("hdfs://master:9000/data/2.gz");
		try {
			inputStream = fileSystem.open(new Path(outputUri)); // 获得源文件输入流
			GzipCodec gzipCodec = new GzipCodec(); // 获得gz格式实例
			gzipCodec.setConf(conf); // 设置Configuration
			compressionOutputStream = gzipCodec.createOutputStream(fileSystem.create(gzPath)); // 创建输出文件获得输出流
			IOUtils.copyBytes(inputStream, compressionOutputStream, 4096, false); // 从输入流复制到输出流 buffsize 4096
			compressionOutputStream.close();			
		} finally {
			// TODO: handle finally clause
			IOUtils.closeStream(inputStream);
			IOUtils.closeStream(outputStream);
		}
		
		
	}
}





