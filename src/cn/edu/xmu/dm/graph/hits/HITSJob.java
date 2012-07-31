package cn.edu.xmu.dm.graph.hits;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HITSJob {
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		String pathIn = "hits/input";// 输入路径
		String pathOut = "";// 输出路径
		// 迭代10次
		for (int i = 0; i < 10; i++) {
			System.out.println("iteration id=" + i);
			Job job = new Job(conf, "MapReduce HITS");
			pathOut = pathIn + i;
			job.setJarByClass(HITSJob.class);
			job.setMapperClass(HITSMapper.class);
			job.setReducerClass(HITSReduce.class);
//			job.setNumReduceTasks(0);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(pathIn));
			FileOutputFormat.setOutputPath(job, new Path(pathOut));
			pathIn = pathOut;
			job.waitForCompletion(true);
		}
	}
}
