package com.zephyx.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import com.zephyx.hadoop.mapreduce.wordcount.WordCountMapper;

public class WordCountMapTest
{
  @Test
  public void processesValidRecord() throws IOException, InterruptedException
  {
    Text value = new Text("5");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
				.withMapper(new WordCountMapper())
				.withInput(new LongWritable(1), value)
				.withOutput(new Text("Odd"), new IntWritable(1))
				.runTest();
	}

  @Test
  public void anotherTest() throws IOException, InterruptedException
  {
  	Text value = new Text("3 7 9 2 4");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new WordCountMapper())
		.withInput(new LongWritable(1), value)
		.withOutput(new Text("Odd"), new IntWritable(1))
		.withOutput(new Text("Odd"), new IntWritable(1))
		.withOutput(new Text("Odd"), new IntWritable(1))
		.withOutput(new Text("Even"), new IntWritable(1))
		.withOutput(new Text("Even"), new IntWritable(1))
		.runTest(true); // true - order of words. Try with false and change order above.
	}
  
}
