package com.mindstix.hadoop.mapreduce.numeric.summarization.average;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageDriver extends Configured implements Tool
{

  public int run(String[] args) throws Exception
  {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    Job job = Job.getInstance(getConf(), "My Average Example");
    job.setJarByClass(AverageDriver.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(AverageMapper.class);
    job.setReducerClass(AverageReducer.class);
    job.setCombinerClass(AverageReducer.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(CountAverageTuple.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new AverageDriver(), args);
    System.exit(exitCode);
  }

}
