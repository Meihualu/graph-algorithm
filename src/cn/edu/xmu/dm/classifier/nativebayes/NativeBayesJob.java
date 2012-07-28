package cn.edu.xmu.dm.classifier.nativebayes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NativeBayesJob {
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		String pathIn = "nativebayes/input";// 输入路径
		String pathOut = "nativebayes/output";// 输出路径

		Job job = new Job(conf, "MapReduce NativeBayes");
		job.setJarByClass(NativeBayesJob.class);
		job.setMapperClass(NativeBayesMapper.class);
		job.setReducerClass(NativeBayesReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(pathIn));
		FileOutputFormat.setOutputPath(job, new Path(pathOut));
		job.waitForCompletion(true);

	}
}
