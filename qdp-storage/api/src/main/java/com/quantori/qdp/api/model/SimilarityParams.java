package com.quantori.qdp.api.model;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

/**
 * Similarity parameters, includes similarity algorithm and its input.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SimilarityParams {
  private SimilarityMetric metric;
  private float minSim;
  private float maxSim;
  private float alpha;
  private float beta;

  /**
   * Supported enumeration of similarity algorithms.
   */
  @Getter
  @RequiredArgsConstructor
  public enum SimilarityMetric {
    /**
     * The <em>tanimoto</em> similarity algorithm.
     *
     * @see <a href="https://en.wikipedia.org/wiki/Chemical_similarity">external:tanimoto</a>
     */
    tanimoto("tanimoto"),
    /**
     * The <em>tversky</em> similarity algorithm.
     *
     * @see <a href="https://www.daylight.com/meetings/mug97/Bradshaw/MUG97/tv_tversky.html">external:tversky</a>
     */
    tversky("tversky"),
    /**
     * The <em>euclid</em> similarity algorithm.
     *
     * @see <a href="https://www.baeldung.com/cs/euclidean-distance-vs-cosine-similarity">external:euclid distance</a>
     */
    euclid("euclid-cub"),
    /**
     * Unknown type of similarity algorithm.
     */
    none("");

    @JsonValue
    private final String value;

  }
}
