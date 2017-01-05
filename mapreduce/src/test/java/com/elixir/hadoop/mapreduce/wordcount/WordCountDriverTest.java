package com.elixir.hadoop.mapreduce.wordcount;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.zephyx.hadoop.mapreduce.wordcount.WordCountDriver;

public class WordCountDriverTest {

	@Test
	public void test() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		conf.setInt("mapreduce.task.io.sort.mb", 1);
		Path input = new Path("resources/data/input/wc.txt");
		Path output = new Path("target/data/output/DriverTest");
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true); // delete old output
		WordCountDriver driver = new WordCountDriver();
		driver.setConf(conf);
		int exitCode = driver.run(new String[] {
		input.toString(), output.toString() });
		Assert.assertEquals(exitCode, 0);
//		checkOutput(conf, output);
	}

}
