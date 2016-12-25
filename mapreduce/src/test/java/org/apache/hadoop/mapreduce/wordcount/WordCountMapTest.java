package org.apache.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

public class WordCountMapTest
{
  @Test
  public void processesValidRecord() throws IOException, InterruptedException
  {
    Text value = new Text("Hadoop");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
				.withMapper(new WordCountMapper())
				.withInput(new LongWritable(1), value)
				.withOutput(value, new IntWritable(1))
				.runTest();
	}

  @Test
  public void anotherTest() throws IOException, InterruptedException
  {
  	Text value = new Text("Hadoop Fundamentals Hadoop is Great");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new WordCountMapper())
		.withInput(new LongWritable(1), value)
		.withOutput(new Text("Hadoop"), new IntWritable(1))
		.withOutput(new Text("Fundamentals"), new IntWritable(1))
		.withOutput(new Text("Hadoop"), new IntWritable(1))
		.withOutput(new Text("is"), new IntWritable(1))
		.withOutput(new Text("Great"), new IntWritable(1))
		.runTest(true); // true - order of words. Try with false and change order above.
	}
  
}
