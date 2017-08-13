package SiteSimilarityCalculator.SitesSimilarityCountSorter;

import org.apache.hadoop.mapreduce.Partitioner;

public class SitesPairsPartition
	extends Partitioner<SitesCommonTagsCount, SitesCommonTagsCount> {

	@Override
	public int getPartition(SitesCommonTagsCount key, SitesCommonTagsCount value, int numberOfPartitions) {
		return key.getPartitionedSite().hashCode() % numberOfPartitions;
	}

}