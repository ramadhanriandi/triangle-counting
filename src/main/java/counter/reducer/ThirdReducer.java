package counter.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdReducer extends Reducer<IntWritable, LongWritable, Text, LongWritable> {
  public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long sumClosedTripletCount = 0;
    Iterator<LongWritable> iterator = values.iterator();

    // sum all count of closed triplet
    while (iterator.hasNext()) {
      LongWritable value = iterator.next();
      sumClosedTripletCount += value.get();
    }

    context.write(new Text("Number of Closed Triplet:"), new LongWritable(sumClosedTripletCount));
  }
}