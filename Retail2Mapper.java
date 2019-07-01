
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Retail2Mapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
  String line = value.toString();
  StringTokenizer token = new StringTokenizer(line);
  int a=0;
  while(token.hasMoreTokens())
{
	String id = token.nextToken();
	
	switch(id){
		case "39": a=a+1;
			   break;
		case "48": a=a+1;
			   break;
	}
}
         if(a >= 2)
{
  context.write(new Text("Both_39_48"), new IntWritable(1));
}
else
{
return;
}
    
  }
}
// ^^ Retail2Mapper