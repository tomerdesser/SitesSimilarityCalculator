package SiteSimilarityCalculator.SitesSimilarityCountSorter;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SitesPairGroupingComparator 
	extends WritableComparator{
	
	public SitesPairGroupingComparator(){
		super(SitesCommonTagsCount.class,true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		SitesCommonTagsCount sitesCommonTagsCount1 = (SitesCommonTagsCount)w1;
		SitesCommonTagsCount sitesCommonTagsCount2 = (SitesCommonTagsCount)w2;
		return sitesCommonTagsCount1.getPartitionedSite().compareTo(sitesCommonTagsCount2.getPartitionedSite());
	}
}
