package SiteSimilarityCalculator.SitesPairing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SitesPairingReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int count = 0;
        for (IntWritable ignored : values)
            count++;

        context.write(key, new IntWritable(count));
    }
}
