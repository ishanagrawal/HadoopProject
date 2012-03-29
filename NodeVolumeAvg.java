package org.myorg;
 
import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class NodeVolumeAvg {
 
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static Text current_location = new Text();	//point id
    private static DoubleWritable speed = new DoubleWritable();	//point current speed

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

      String file = value.toString();
      String lines[] = file.split("\n");
      NodeParser parser= new NodeParser();

      for(int i=0; i< lines.length; i++)
	  {
      	parser.parse(lines[i]);
      
		current_location.set(parser.getXCurrent() + ":" + parser.getYCurrent());
		speed.set(parser.getSpeed());
		
		output.collect(current_location, speed);
	  }

    }
  }
 
  public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, Text> {
    private static Text vol_speed = new Text();	//the volume and speed through a node 

    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      double sum = 0, avg;
      int count = 0;
      while (values.hasNext()) {
        sum += values.next().get();
		count++;
      }
      avg = sum/count;
      vol_speed.set(count + "\t" + avg);
      output.collect(key,  vol_speed);
    }
  }
 
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(NodeVolumeAvg.class);
    conf.setJobName("NodeVolumeAvg");
 
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(DoubleWritable.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    
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
