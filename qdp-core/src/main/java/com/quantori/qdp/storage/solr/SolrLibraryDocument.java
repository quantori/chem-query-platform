package com.quantori.qdp.storage.solr;

import java.time.ZonedDateTime;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.solr.client.solrj.beans.Field;

@Getter
@ToString
@AllArgsConstructor
class SolrLibraryDocument {

  @Field("id")
  private final String id;

  @Field("name")
  private final String name;

  @Field("type")
  private final String type;

  @Field("structures_count")
  private final long size;

  @Field("created_timestamp")
  private final ZonedDateTime createdStamp;
}
