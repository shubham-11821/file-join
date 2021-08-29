package com.raj.filejoin;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileJoinReducer extends Reducer<Text, JoinWritable, NullWritable, Text> {
	public void reduce(Text key, Iterable<JoinWritable> values, Context context)
			throws IOException, InterruptedException {
		String name = "";
		String dept = "";
		String salary = "";
		StringBuffer val = new StringBuffer().append(key.toString()).append(",");
		for (JoinWritable j : values) {
			if (j.getfilename().equals("empname.txt"))
				name = j.getvalue();
			else if (j.getfilename().equals("empdept.txt"))
				dept = j.getvalue();
			else if (j.getfilename().equals("empsal.txt"))
				salary = j.getvalue();
		}
		val.append(name).append(",").append(dept).append(",").append(salary);
		context.write(NullWritable.get(), new Text(val.toString()));
	}
}
