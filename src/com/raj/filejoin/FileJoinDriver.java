package com.raj.filejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FileJoinDriver {
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"File Join");
		job.setJarByClass(FileJoinDriver.class);
		job.setMapperClass(FileJoinMapper.class);
		job.setReducerClass(FileJoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JoinWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/raj/DF/Research/MapReduce/FileJoin/input/empdept.txt"));
		FileInputFormat.addInputPath(job, new Path("/home/raj/DF/Research/MapReduce/FileJoin/input/empname.txt"));
		FileInputFormat.addInputPath(job, new Path("/home/raj/DF/Research/MapReduce/FileJoin/input/empsal.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/home/raj/DF/Research/MapReduce/FileJoin/output"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
