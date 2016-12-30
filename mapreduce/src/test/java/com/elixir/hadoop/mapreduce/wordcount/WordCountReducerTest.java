package com.elixir.hadoop.mapreduce.wordcount;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.elixir.hadoop.mapreduce.wordcount.WordCountReducer;

public class WordCountReducerTest {

	Text key = new Text("Elixir");
	@Test
	public void testRudeduce() throws IOException, InterruptedException{
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new WordCountReducer())
		.withInput(key, Arrays.asList(new IntWritable(1), new IntWritable(3)))
		.withOutput(key, new IntWritable(4));
	}
}
