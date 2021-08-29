package com.raj.filejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileJoinMapper extends Mapper<LongWritable, Text, Text, JoinWritable>{
	String filename;
	public void setup(Context context)
	{
		FileSplit filesplit = (FileSplit) context.getInputSplit();
		filename = filesplit.getPath().getName();
	}
	public void  map(LongWritable k, Text values, Context context) throws IOException,InterruptedException
	{
		String data[] = values.toString().split(",");
		if(data.length==2)
		{
			context.write(new Text(data[0]), new JoinWritable(data[1], filename));
		}
	}

}
