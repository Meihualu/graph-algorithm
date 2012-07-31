package cn.edu.xmu.dm.graph.hits;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HITSMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) {
		
		String pageNAuthNHubN = "";
		String inListPageN = "";
		String outListPageN = "";
		StringTokenizer str = new StringTokenizer(value.toString());

		if (str.hasMoreTokens()) {
			pageNAuthNHubN = str.nextToken();
			System.out.println("pageNAuthNHubN: " + pageNAuthNHubN);
		}
		if (str.hasMoreTokens()){
			inListPageN = str.nextToken();
			System.out.println("inListPageN: " + inListPageN);
		}
		if (str.hasMoreTokens()){
			outListPageN = str.nextToken();
			System.out.println("outListPageN: " + outListPageN);
		}
		
		String[] pageNAuthNHubNs = pageNAuthNHubN.split(","); 
		String[] outListPageNs = outListPageN.split(",");
		String[] inListPageNs = inListPageN.split(",");

		for(int i = 0; i < outListPageNs.length; i++){
			try {
				context.write(new Text(outListPageNs[i]), new Text("H"+pageNAuthNHubNs[0]+","+pageNAuthNHubNs[2]));
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			context.write(new Text(pageNAuthNHubNs[0]), new Text("HL"+outListPageN));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		for(int i = 0; i < inListPageNs.length; i++){
			try {
				context.write(new Text(inListPageNs[i]), new Text("A"+pageNAuthNHubNs[0]+","+pageNAuthNHubNs[1]));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			context.write(new Text(pageNAuthNHubNs[0]), new Text("AL"+inListPageN));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}