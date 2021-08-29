package com.raj.filejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class JoinWritable implements Writable{
	private Text value;
	private Text filename;
	
	public JoinWritable() {
		// TODO Auto-generated constructor stub
		set(new Text(),new Text());
	}
	public JoinWritable(Text value, Text filename)
	{
		set(value,filename);
	}
	public JoinWritable(String value, String filename)
	{
		set(new Text(value),new Text(filename));
	}
	public String getvalue() {return this.value.toString();}
	public String getfilename() {return this.filename.toString();}
	public void set(Text value, Text filename) {
		// TODO Auto-generated method stub
		this.value=value;
		this.filename=filename;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value.readFields(in);
		filename.readFields(in);
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		value.write(out);
		filename.write(out);
		
	}

}
