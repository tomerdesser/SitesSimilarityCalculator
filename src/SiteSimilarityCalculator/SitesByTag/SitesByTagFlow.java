package SiteSimilarityCalculator.SitesByTag;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import SiteSimilarityCalculator.IMapReduceFlow;
import SiteSimilarityCalculator.SitesSimilarityCalculator;

import java.io.IOException;

public class SitesByTagFlow implements IMapReduceFlow {
    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job();
        job.setJobName("Aggregate Sites By Tag");
        job.setJarByClass(SitesSimilarityCalculator.class);

        job.setMapperClass(SitesByTagMapper.class);
        job.setReducerClass(SitesByTagReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("temp/1"));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
