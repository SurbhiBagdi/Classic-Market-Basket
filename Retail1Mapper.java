
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Retail1Mapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
  String line = value.toString();
  StringTokenizer token = new StringTokenizer(line);
  int tk = token.countTokens();
  String outkey= Integer.toString(tk);
  context.write(new Text(outkey), new IntWritable(1));

    
  }
}
// ^^ Retail1Mapper