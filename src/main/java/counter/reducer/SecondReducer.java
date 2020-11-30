package counter.reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<Text, Text, IntWritable, LongWritable> {
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    boolean isConnected = false;
    long count = 0;
    Set<String> valueSet = new HashSet<>();

    for (Text value : values) {
      valueSet.add(value.toString());
    }

    for (String value : valueSet) {
      if (value.equals("$")) isConnected = true;
      else count++;
    }

    if (isConnected) context.write(new IntWritable(0), new LongWritable(count));
  }
}