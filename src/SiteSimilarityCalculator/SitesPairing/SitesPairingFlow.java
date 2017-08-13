package SiteSimilarityCalculator.SitesPairing;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import SiteSimilarityCalculator.IMapReduceFlow;
import SiteSimilarityCalculator.SitesSimilarityCalculator;

import java.io.IOException;

public class SitesPairingFlow implements IMapReduceFlow {
    @Override
    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Job job = new Job();
        job.setJobName("Pair Sites With Same Tag");

        job.setJarByClass(SitesSimilarityCalculator.class);

        job.setMapperClass(SitesPairingMapper.class);
        job.setReducerClass(SitesPairingReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("temp/1"));
        FileOutputFormat.setOutputPath(job, new Path("temp/2"));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
