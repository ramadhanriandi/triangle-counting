package counter.mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
  public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
    String[] edge = text.toString().split("\\s+");  // pair of user id - follower id

    long vertexX = Long.parseLong(edge[0]); // user id
    long vertexY = Long.parseLong(edge[1]); // follower id

    // do preprocess by putting the lower id in the left side
    if (vertexX > vertexY) context.write(new LongWritable(vertexY), new LongWritable(vertexX));
    else context.write(new LongWritable(vertexX), new LongWritable(vertexY));
  }
}

