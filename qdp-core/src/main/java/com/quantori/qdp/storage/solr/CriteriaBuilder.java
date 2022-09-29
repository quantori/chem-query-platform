package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.SearchProperty;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.experimental.UtilityClass;
import org.apache.solr.client.solrj.util.ClientUtils;

@UtilityClass
class CriteriaBuilder {

  static String buildQueryString(List<SearchProperty> properties) {
    return properties.stream()
        .map(CriteriaBuilder::buildProperty)
        .collect(Collectors.joining(" AND "));
  }

  private static String buildProperty(SearchProperty property) {
    return Arrays.stream(Criteria.values())
        .filter(command -> command.operator.equals(property.getLogicalOperator()))
        .findAny()
        .orElse(Criteria.UNKNOWN)
        .buildCriteria(property.getProperty().trim(), property.getValue().trim());
  }

  @AllArgsConstructor
  private enum Criteria {
    EQUALS("=", "%s:\"%s\""),
    LESS("<", "%s:[* TO %s}"),
    LESS_OR_EQUAL("<=", "%s:[* TO %s]"),
    GREATER(">", "%s:{%s TO *]"),
    GREATER_OR_EQUAL(">=", "%s:[%s TO *]"),
    UNKNOWN("", "");
    private final String operator;
    private final String criteriaTemplate;

    private String buildCriteria(String key, String value) {
      return String.format(criteriaTemplate, ClientUtils.escapeQueryChars(key), ClientUtils.escapeQueryChars(value));
    }
  }
}
