package com.quantori.cqp.api.model;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

/**
 * Similarity parameters of a search request.
 *
 * <p>Includes similarity algorithm and its input.
 */
@Jacksonized
@SuperBuilder
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class SimilarityParams extends SearchParams {
  private SimilarityMetric metric;
  private float minSim;
  private float maxSim;
  private float alpha;
  private float beta;

  /** Supported enumeration of similarity algorithms. */
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
     * @see <a
     *     href="https://www.daylight.com/meetings/mug97/Bradshaw/MUG97/tv_tversky.html">external:tversky</a>
     */
    tversky("tversky"),
    /**
     * The <em>euclid</em> similarity algorithm.
     *
     * @see <a
     *     href="https://www.baeldung.com/cs/euclidean-distance-vs-cosine-similarity">external:euclid
     *     distance</a>
     */
    euclid("euclid-cub"),
    /** Unknown type of similarity algorithm. */
    none("");

    @JsonValue private final String value;
  }
}
