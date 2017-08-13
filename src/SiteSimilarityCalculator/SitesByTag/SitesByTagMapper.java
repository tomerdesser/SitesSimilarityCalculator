package SiteSimilarityCalculator.SitesByTag;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SitesByTagMapper extends Mapper<LongWritable, Text, Text, Text>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] words = value.toString().split(" ") ;
        context.write(new Text(words[1]), new Text(words[0]));
    }
}
