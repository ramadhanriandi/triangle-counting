package counter.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
  public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
    String[] counter = text.toString().split("\\s+");

    IntWritable parsedKey = new IntWritable(Integer.parseInt(counter[0]));
    LongWritable parsedCount = new LongWritable(Long.parseLong(counter[1]));

    context.write(parsedKey, parsedCount);
  }
}