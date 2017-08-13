package SiteSimilarityCalculator.SitesSimilarityCountSorter;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer
	extends Reducer<SitesCommonTagsCount,SitesCommonTagsCount, Text, IntWritable> {
	
	@Override
	protected void reduce(SitesCommonTagsCount key, Iterable<SitesCommonTagsCount> values, Context context) throws IOException, InterruptedException {
		int pairsMaxCount = 10;

		for (SitesCommonTagsCount value : values){
			if (pairsMaxCount == 0)
				break;

			context.write(value.getCountKey(), value.getCountValue());
			pairsMaxCount--;
		}
	}
}







