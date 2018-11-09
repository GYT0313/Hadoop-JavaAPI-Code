package wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author: Gu Yongtao
 * @Description: 
 * @date: 2018年11月6日 下午4:53:59
 * @Filename: WordMain.java
 */

public class WordMain {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// Configuration类：读取配置文件内容-core-site.xml
		Configuration conf = new Configuration();
		
		// 读取命令行参数，并设置到conf
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// 添加内容
		conf.set("fs.defaultFS", "hdfs://10.13.7.72:9000");
		conf.set("hadoop.job.user", "hadoop");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.jobtracker.address", "10.13.7.72:9001");
		conf.set("yarn.resourcemanager.hostname", "10.13.7.72");
		conf.set("yarn.resourcemanager.admin.adress", "10.13.7.72:8033");
		conf.set("yarn.resourcemanager.adress", "10.13.7.72:8032");
		conf.set("yarn.resourcemanager.resource-tracker.address", "10.13.7.72:8036");
		conf.set("yarn.resourcemanager.scheduler.address", "10.13.7.72:8030");
		
		
		if (otherArgs.length != 2) { // 输入目录 输出目录
			System.err.println("Usage: wordcount <in><out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "word count"); // 新建一个job
		job.setJar("wordcount.jar");
		
		job.setJarByClass(WordMain.class); // 设置主类
		job.setMapperClass(WordMapper.class); // 设置Mapper类
		job.setCombinerClass(WordReducer.class); // 设置作业合成类
		job.setReducerClass(WordReducer.class); // 设置Reducer类
		job.setOutputKeyClass(Text.class); // 设置输出数据的关键类
		job.setOutputValueClass(IntWritable.class); // 设置输出值类
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}





