package com.zephyx.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
  IntWritable one = new IntWritable(1);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    String[] numbers = value.toString().split(" ");
    for (String num : numbers) {
      int i = Integer.parseInt(num);
      if (i%2==0)
        context.write(new Text("Even"), new IntWritable(1));
      else
        context.write(new Text("Odd"), new IntWritable(1));
    }
  }
}
