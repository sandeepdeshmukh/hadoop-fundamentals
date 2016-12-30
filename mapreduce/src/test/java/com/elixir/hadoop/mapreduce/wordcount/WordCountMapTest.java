package com.elixir.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

import com.elixir.hadoop.mapreduce.wordcount.WordCountMapper;

public class WordCountMapTest {
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		Text value = new Text("Elixir");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
				.withMapper(new WordCountMapper())
				.withInput(new LongWritable(1), value)
				.withOutput(value, new IntWritable(1))
				.runTest();
	}
	
	@Test
	public void anotherTest() throws IOException, InterruptedException {
		Text value = new Text("Elixir Technologies");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new WordCountMapper())
		.withInput(new LongWritable(1), value)
		.withOutput(new Text("Elixir"), new IntWritable(1))
		.withOutput(new Text("Technologies"), new IntWritable(1))
		.runTest(true);;
	}
	
}
