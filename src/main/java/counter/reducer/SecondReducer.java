package counter.reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<Text, Text, IntWritable, LongWritable> {
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    boolean isClose = false;
    long countTriangleCandidates = 0;
    Iterator<Text> iterator = values.iterator();
    Set<String> valueSet = new HashSet<>();

    // use Set to prevent redundancy
    while (iterator.hasNext()) {
      Text value = iterator.next();
      valueSet.add(value.toString());
    }

    // count all triangle candidates
    for (String value : valueSet) {
      if (value.equals("$")) isClose = true;  // indicate that the triangle is a closed triplet
      else countTriangleCandidates++;         // count if the value is an id
    }

    // only emit the count when the triangle is truly a closed triplet
    if (isClose && countTriangleCandidates > 0) context.write(new IntWritable(0), new LongWritable(countTriangleCandidates));
  }
}