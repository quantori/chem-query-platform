package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.quantori.cqp.core.model.SimilarityParams;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
class ElasticScripts {

  private static final String TANIMOTO_SCRIPT = "double dotp = dotProduct(params.sim,'sim');"
      + " return dotp/(params.nonZero + l1norm(params.zero, 'sim') - dotp)";

  private static final String EUCLID_SCRIPT = "dotProduct(params.sim,'sim')/params.nonZero";

  private static final String TVERSKY_SCRIPT = "double dotp = dotProduct(params.sim,'sim');"
      + "return dotp/(params.alpha*(params.nonZero - dotp) + params.beta*(l1norm(params.zero, 'sim') - dotp) + dotp)";

  public static void update(ElasticsearchAsyncClient client) {
    process(client, SimilarityParams.SimilarityMetric.tanimoto.getValue(), TANIMOTO_SCRIPT);
    process(client, SimilarityParams.SimilarityMetric.euclid.getValue(), EUCLID_SCRIPT);
    process(client, SimilarityParams.SimilarityMetric.tversky.getValue(), TVERSKY_SCRIPT);
  }

  private static void process(ElasticsearchAsyncClient client, String name, String source) {
    client.putScript(req -> req.id(name).script(s -> s.lang("painless").source(source)))
        .whenComplete((response, exception) -> {
          if (exception != null) {
            log.error("Script {} failed", name, exception);
          } else {
            log.info("script {} updated", name);
          }
        }).join();
  }
}
