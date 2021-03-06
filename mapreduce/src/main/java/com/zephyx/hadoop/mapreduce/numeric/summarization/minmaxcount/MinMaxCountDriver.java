package com.zephyx.hadoop.mapreduce.numeric.summarization.minmaxcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MinMaxCountDriver extends Configured implements Tool
{

  public int run(String[] args) throws Exception
  {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    Job job = Job.getInstance(getConf(), "My Word Count Example");
    job.setJarByClass(MinMaxCountDriver.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(MinMaxCountMapper.class);
    job.setReducerClass(MinMaxCountReducer.class);
    job.setCombinerClass(MinMaxCountReducer.class);
    job.setNumReduceTasks(3);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(MinMaxCountTuple.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new MinMaxCountDriver(), args);
    System.exit(exitCode);
  }

}
