package SiteSimilarityCalculator.SitesSimilarityCountSorter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import SiteSimilarityCalculator.IMapReduceFlow;
import SiteSimilarityCalculator.SitesSimilarityCalculator;

import java.io.IOException;

public class PairsSortFlow implements IMapReduceFlow {

    @Override
    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort Sites By Name And Count");
        job.setJarByClass(SitesSimilarityCalculator.class);
        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);

        job.setMapOutputKeyClass(SitesCommonTagsCount.class);
        job.setMapOutputValueClass(SitesCommonTagsCount.class);

        job.setGroupingComparatorClass(SitesPairGroupingComparator.class);
        job.setPartitionerClass(SitesPairsPartition.class);

        FileInputFormat.addInputPath(job, new Path("temp/2"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }
}
