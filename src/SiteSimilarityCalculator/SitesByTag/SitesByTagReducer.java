package SiteSimilarityCalculator.SitesByTag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SitesByTagReducer extends Reducer<Text, Text, Text, IntWritable>
{
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        StringBuilder sb = new StringBuilder();

        for (Text val : values)
        {
            sb.append(val + " ");
        }

        result.set(sum);
        context.write(new Text(sb.toString().trim()), new IntWritable(1));
    }
}
