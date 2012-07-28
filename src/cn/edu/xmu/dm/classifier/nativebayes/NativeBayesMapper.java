package cn.edu.xmu.dm.classifier.nativebayes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NativeBayesMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private int classIndex = 4;
	public void map(LongWritable key, Text value, Context context) {
		String[] splits = value.toString().split(",");
		
		for(int i = 0; i < classIndex; i++){
			try {
				context.write(new Text(splits[classIndex] + "_" + i), new Text(splits[i]));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	
		try {
			context.write(new Text("target_" + splits[classIndex]), new Text("1"));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
}