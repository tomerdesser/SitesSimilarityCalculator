package SiteSimilarityCalculator.SitesPairing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SitesPairingMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] matchingSites = value.toString().split("\t")[0].split(" ");

        for(String siteA : matchingSites)
            for(String siteB : matchingSites)
                if(!siteA.equals(siteB))
                    context.write(new Text(siteA + " " + siteB), new IntWritable(1));
    }
}
