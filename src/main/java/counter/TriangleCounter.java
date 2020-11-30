package counter;

import counter.mapper.FirstMapper;
import counter.mapper.SecondMapper;
import counter.mapper.ThirdMapper;
import counter.reducer.FirstReducer;
import counter.reducer.SecondReducer;
import counter.reducer.ThirdReducer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class TriangleCounter extends Configured {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job firstJob = Job.getInstance(conf, "FirstPhase");
    Job secondJob = Job.getInstance(conf, "SecondPhase");
    Job thirdJob = Job.getInstance(conf, "ThirdPhase");

    firstJob.setJarByClass(TriangleCounter.class);
    secondJob.setJarByClass(TriangleCounter.class);
    thirdJob.setJarByClass(TriangleCounter.class);

    thirdJob.setNumReduceTasks(1);

    firstJob.setMapperClass(FirstMapper.class);
    secondJob.setMapperClass(SecondMapper.class);
    thirdJob.setMapperClass(ThirdMapper.class);

    firstJob.setReducerClass(FirstReducer.class);
    secondJob.setReducerClass(SecondReducer.class);
    thirdJob.setReducerClass(ThirdReducer.class);

    firstJob.setMapOutputKeyClass(LongWritable.class);
    firstJob.setMapOutputValueClass(LongWritable.class);
    secondJob.setMapOutputKeyClass(Text.class);
    secondJob.setMapOutputValueClass(Text.class);
    thirdJob.setMapOutputKeyClass(IntWritable.class);
    thirdJob.setMapOutputValueClass(LongWritable.class);

    firstJob.setOutputKeyClass(Text.class);
    firstJob.setOutputValueClass(Text.class);
    secondJob.setOutputKeyClass(IntWritable.class);
    secondJob.setOutputValueClass(LongWritable.class);
    thirdJob.setOutputKeyClass(Text.class);
    thirdJob.setOutputValueClass(LongWritable.class);

    FileInputFormat.addInputPath(firstJob, new Path(args[0]));
    FileOutputFormat.setOutputPath(firstJob, new Path(args[1]));
    FileInputFormat.addInputPath(secondJob, new Path(args[1]));
    FileOutputFormat.setOutputPath(secondJob, new Path(args[2]));
    FileInputFormat.addInputPath(thirdJob, new Path(args[2]));
    FileOutputFormat.setOutputPath(thirdJob, new Path(args[3]));

    System.exit(firstJob.waitForCompletion(true) && secondJob.waitForCompletion(true) && thirdJob.waitForCompletion(true) ? 0 : 1);
  }
}