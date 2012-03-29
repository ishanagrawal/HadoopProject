package org.myorg;
 
import java.io.IOException;
import java.util.*;
import java.lang.Math;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class PointDevSpeed {
 
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    private static IntWritable id = new IntWritable();	//point id
    private static DoubleWritable speed = new DoubleWritable();	//point current speed

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {

      String file = value.toString();
      String lines[] = file.split("\n");
      DefaultParser parser= new DefaultParser();

      for(int i=0; i< lines.length; i++)
	{
      	parser.parse(lines[i]);
      
	id.set(parser.getPointId());
	speed.set(parser.getSpeed());
	output.collect(id, speed);
	}

    }
  }
 
  public static class Reduce extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    private static DoubleWritable deviation = new DoubleWritable();	//point avg speeg 

    public void reduce(IntWritable key, Iterator<DoubleWritable> values, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
      double sum = 0, val, sum2=0, avg;
      int count = 0;
      while (values.hasNext()) {
        val = values.next().get();
	sum += val;
	sum2 += val * val;
	count++;
      }
      avg = sum/count;
      deviation.set(Math.sqrt( sum2/count - (avg*avg) ));
      output.collect(key, deviation);
    }
  }
 
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(PointDevSpeed.class);
    conf.setJobName("PointDevSpeed");
 
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(DoubleWritable.class);
    
    conf.setNumReduceTasks(4);
    //conf.setNumMapTasks(4);
 
    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
 
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
 
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
    JobClient.runJob(conf);
  }
}
