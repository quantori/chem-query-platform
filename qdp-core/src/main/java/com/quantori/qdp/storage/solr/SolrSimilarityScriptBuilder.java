package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.FingerPrintUtilities;
import com.quantori.qdp.storage.api.Similarity;

class SolrSimilarityScriptBuilder {
  public static final int DEFAULT_SEARCH_PAGE_SIZE = 5000;

  private final String simHash;
  private final Similarity.SimilarityMetric metric;
  private final float minScore;
  private final float maxScore;
  private final double alpha;
  private final double beta;

  SolrSimilarityScriptBuilder(byte[] simFingerprint, Similarity.SimilarityMetric metric, float minScore, float maxScore,
                              double alpha, double beta) {
    this.simHash = FingerPrintUtilities.similarityHash(simFingerprint);
    this.metric = metric;
    this.maxScore = maxScore;
    this.minScore = minScore;
    this.alpha = alpha;
    this.beta = beta;
  }

  private static String tanimotoSimilarity(String simHash) {
    return String.format("tanimoto('%s',sim)", simHash);
  }

  private static String euclidSimilarity(String simHash) {
    return String.format("euclidean('%s',sim)", simHash);
  }

  private static String tverskyScript(String simHash, final double alpha, final double beta) {
    return String.format("tversky('%s',%,.2f,%,.2f,sim)", simHash, alpha, beta);
  }

  private String generateFilterQuery(final String script, final float minScore, final float maxScore) {
    return "{!frange l="
        + minScore
        + " u="
        + maxScore + "}" + script;
  }

  String build() {
    var formula = switch (metric) {
      case tanimoto -> tanimotoSimilarity(simHash);
      case euclid -> euclidSimilarity(simHash);
      case tversky -> tverskyScript(simHash, alpha, beta);
      default -> "";
    };
    return generateFilterQuery(formula, minScore, maxScore);
  }
}
