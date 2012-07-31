package cn.edu.xmu.dm.graph.hits;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HITSReduce extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) {

		System.err.print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^" + key.toString()
				+ ":");
		String outListPageK = "";
		String inListPageK = "";

		Map<String, String> pageAndValue = new HashMap<String, String>();

		Double authK = 0.0;
		Double hubK = 0.0;

		String strKey = key.toString();
		System.out.println("strKey: " + strKey);
		String page = strKey;
		System.out.println("page: " + page);
		for (Text value : values) {
			String strValue = value.toString();
			System.out.print(value.toString() + "\t");

			if (strValue.contains("HL")) {
				outListPageK = strValue.substring(strValue.indexOf("HL") + 2);
				System.out.println("outListPageK:" + outListPageK);
				continue;
			} else if(!strValue.contains("HL") && !strValue.contains("AL")) {
				strValue = strValue.substring(1);
				
				pageAndValue
						.put(strValue.split(",")[0], strValue.split(",")[1]);
			}

			if (strValue.contains("AL")) {
				inListPageK = strValue.substring(strValue.indexOf("AL") + 2);
				System.out.println("inListPageK:" + inListPageK);
				continue;
			} else if(!strValue.contains("HL") && !strValue.contains("AL")) {
				strValue = strValue.substring(1);
				pageAndValue
						.put(strValue.split(",")[0], strValue.split(",")[1]);
			}
		}

		 System.out.println("print Hash:");
		 pageAndValue.remove("");
		 Iterator iter = pageAndValue.entrySet().iterator();
		 while (iter.hasNext()) {
		 java.util.Map.Entry entry = (Map.Entry) iter.next();
		 System.out.println(entry.getKey() + ":" + entry.getValue());
		 }

		String[] outListPageKs = outListPageK.split(",");
		double hubKSum = 0.0;
		for (int i = 0; i < outListPageKs.length; i++) {
			authK += Double.parseDouble(pageAndValue.get(outListPageKs[i]));
			hubKSum += Double.parseDouble(pageAndValue.get(outListPageKs[i])) * Double.parseDouble(pageAndValue.get(outListPageKs[i]));;
		}
		authK /= Math.sqrt(hubKSum);
		
		String[] inListPageKs = inListPageK.split(",");
		double authkSum = 0.0;
		for (int i = 0; i < inListPageKs.length; i++) {
			hubK += Double.parseDouble(pageAndValue.get(inListPageKs[i]));
			authkSum += Double.parseDouble(pageAndValue.get(inListPageKs[i])) * Double.parseDouble(pageAndValue.get(inListPageKs[i]));
		}
		hubK /= Math.sqrt(authkSum);
		StringBuffer sbKey = new StringBuffer();
		StringBuffer sbValue = new StringBuffer();
		sbKey.append(page).append(",").append(authK.toString()).append(",")
				.append(hubK.toString());

		sbValue.append(inListPageK).append("\t").append(outListPageK);
		try {
			context.write(new Text(sbKey.toString()),
					new Text(sbValue.toString()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println();
	}
}
