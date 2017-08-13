package SiteSimilarityCalculator.SitesSimilarityCountSorter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable, Text, SitesCommonTagsCount, SitesCommonTagsCount>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] keyAndCount = value.toString().split("\t");
		String[] sitesPair = keyAndCount[0].split(" ");

		SitesCommonTagsCount sitesCommonTagsCount = new SitesCommonTagsCount(sitesPair[0], sitesPair[1], Integer.parseInt(keyAndCount[1]));

		context.write(sitesCommonTagsCount, sitesCommonTagsCount);
	}
}
