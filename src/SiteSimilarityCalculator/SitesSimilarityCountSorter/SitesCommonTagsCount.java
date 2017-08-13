package SiteSimilarityCalculator.SitesSimilarityCountSorter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class SitesCommonTagsCount
	implements Writable, WritableComparable<SitesCommonTagsCount>{
	
	private Text PartitionedSite;
	private Text SortedSite;
	private IntWritable CommonTagsCount;

	//Mandatory for the .class reflection part, intelij made me lose 3 hours of my life for this warning.
	public SitesCommonTagsCount(){
		PartitionedSite = new Text();
		SortedSite = new Text();
		CommonTagsCount = new IntWritable();
	}
	
	public SitesCommonTagsCount(String partitionedSite, String sortedSite, int commonTagsCount){
		this.PartitionedSite = new Text(partitionedSite);
		this.SortedSite = new Text(sortedSite);
		this.CommonTagsCount = new IntWritable(commonTagsCount);
	}
	
	public Text getPartitionedSite(){
		return this.PartitionedSite;
	}

	@Override
	public int compareTo(SitesCommonTagsCount o) {
		int cmp = this.PartitionedSite.compareTo(o.PartitionedSite);
		if (cmp == 0)
			cmp = this.CommonTagsCount.compareTo(o.CommonTagsCount) * (-1);

		return cmp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.PartitionedSite.readFields(in);
		this.SortedSite.readFields(in);
		this.CommonTagsCount.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.PartitionedSite.write(out);
		this.SortedSite.write(out);
		this.CommonTagsCount.write(out);
	}

	public Text getCountKey() {
		return new Text(PartitionedSite + " " + SortedSite);
	}

	public IntWritable getCountValue() {
		return CommonTagsCount;
	}
}
