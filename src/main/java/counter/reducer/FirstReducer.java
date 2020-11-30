package counter.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
  public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    List<Long> valueList = new ArrayList<>();

    for (LongWritable u : values) {
      // emit this to indicate that the vertices are connected
      context.write(new Text(key.toString() + ',' + u.toString()), new Text("$"));

      // add the value to list for looping through the value index
      valueList.add(u.get());
    }

    // generate all triangle vertices group
    for (int i = 0; i < valueList.size(); i++) {
      for (int j = i; j < valueList.size(); j++) {
        // put the lower id in the left side of the key and this reducer input key as the value
        if (valueList.get(i) > valueList.get(j)) context.write(new Text(valueList.get(j).toString() + ',' + valueList.get(i).toString()), new Text(key.toString()));
        else context.write(new Text(valueList.get(i).toString() + ',' + valueList.get(j).toString()), new Text(key.toString()));
      }
    }
  }
}