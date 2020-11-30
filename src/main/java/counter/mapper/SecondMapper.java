package counter.mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<LongWritable, Text, Text, Text> {
  public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
    String[] connector = text.toString().split("\\s+");

    // contain vertices pair in format of X,Y
    Text keyPair = new Text(connector[0]);

    // contain the vertex with the lowest id in the triangle
    // OR "$" sign which indicates the keyPair is connected
    Text parsedConnector = new Text(connector[1]);

    context.write(keyPair, parsedConnector);
  }
}

