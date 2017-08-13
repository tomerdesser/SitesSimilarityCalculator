package SiteSimilarityCalculator;

import java.io.IOException;

public interface IMapReduceFlow {
    int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException;
}
