package cn.edu.xmu.dm.classifier.nativebayes;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NativeBayesReduce extends
		Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) {
		String strKey = key.toString();
		
		if(strKey.startsWith("target_")){
			Double sum = 0.0;
			for (Text value : values){
				sum += Double.parseDouble(value.toString());
			}
			try {
				context.write(new Text(key.toString()), new Text(sum.toString()));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}else{
			Double sum = 0.0;
			Double sumSq = 0.0;
			
			int count = 0;
			
			for(Text value : values){
				double v = Double.parseDouble(value.toString());
				sum += v;
				sumSq += v * v;
				count++;
			}
			
			Double mean = sum / count;
			Double stddev = Math.abs((sumSq - mean * sum) / count);
			try {
				context.write(new Text(key.toString() + "_mean"), new Text(sum.toString()));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				context.write(new Text(key.toString() + "_stddev"), new Text(stddev.toString()));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
