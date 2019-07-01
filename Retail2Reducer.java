

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Retail2Reducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
	int sum = 0;
    for (IntWritable value : values) {
      sum += value.get();  //#of customers purchased 39& 48 both
    }
    context.write(key, new IntWritable(sum));
  }
}
// ^^ Retail1Reducer