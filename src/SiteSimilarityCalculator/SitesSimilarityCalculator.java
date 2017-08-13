package SiteSimilarityCalculator;

import org.apache.hadoop.conf.Configuration;

import SiteSimilarityCalculator.SitesSimilarityCountSorter.PairsSortFlow;
import SiteSimilarityCalculator.SitesByTag.SitesByTagFlow;
import SiteSimilarityCalculator.SitesPairing.SitesPairingFlow;


public class SitesSimilarityCalculator
{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        IMapReduceFlow[] flows = new IMapReduceFlow[]{new SitesByTagFlow(), new SitesPairingFlow(), new PairsSortFlow()};

        int result = 1;
        for (IMapReduceFlow flow : flows) {
            result = flow.run(args);
        }

        System.exit(result);
    }
}