package counter.reducer;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
  public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    SortedSet<Long> valueSet = new TreeSet<>();
    Map<String, String> emittedConnectedId = new HashMap<>();
    Map<String, String> emittedTriangleCandidate = new HashMap<>();
    Iterator<LongWritable> iterator = values.iterator();

    while (iterator.hasNext()) {
      LongWritable value = iterator.next();

      String connectedId = key.toString() + ',' + value.get();
      // put pair of connected ids into a set if it hasn't been emitted
      if (!emittedConnectedId.containsKey(connectedId)) {
        emittedConnectedId.put(connectedId, "$");

        // emit this to indicate that the vertices are connected
        context.write(new Text(connectedId), new Text("$"));
      }

      // add the value to list for looping through the value index
      valueSet.add(value.get());
    }

    // convert valueSet into a list
    List<Long> valueList = new ArrayList<>(valueSet);

    // generate all triangle vertices group
    for (int i = 0; i < valueList.size(); i++) {
      for (int j = i + 1; j < valueList.size(); j++) {
        if (valueList.get(i) != valueList.get(j)) {
          String verticesPair = valueList.get(i).toString() + ',' + valueList.get(j).toString();

          // put pair of vertices into a set if it hasn't been emitted
          if (!emittedTriangleCandidate.containsKey(verticesPair)) {
            emittedTriangleCandidate.put(verticesPair, key.toString());

            // emit vertices pair as key and the current key as value
            context.write(new Text(verticesPair), new Text(key.toString()));
          }
        }
      }
    }
  }
}