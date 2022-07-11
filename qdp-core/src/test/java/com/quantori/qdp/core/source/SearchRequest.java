package com.quantori.qdp.core.source;

import com.quantori.qdp.core.source.model.ProcessingSettings;
import com.quantori.qdp.core.source.model.RequestStructure;

public class SearchRequest {
  private final ProcessingSettings processingSettings;
  private final RequestStructure requestStructure;

  SearchRequest(ProcessingSettings processingSettings, RequestStructure requestStructure) {
    this.processingSettings = processingSettings;
    this.requestStructure = requestStructure;
  }

  public static SearchRequestBuilder builder() {
    return new SearchRequestBuilder();
  }

  public ProcessingSettings getProcessingSettings() {
    return this.processingSettings;
  }

  public RequestStructure getRequestStructure() {
    return this.requestStructure;
  }

  public static class SearchRequestBuilder {
    private ProcessingSettings processingSettings;
    private RequestStructure requestStructure;

    SearchRequestBuilder() {
    }

    public SearchRequestBuilder processingSettings(ProcessingSettings processingSettings) {
      this.processingSettings = processingSettings;
      return this;
    }

    public SearchRequestBuilder requestStructure(RequestStructure requestStructure) {
      this.requestStructure = requestStructure;
      return this;
    }

    public SearchRequest build() {
      return new SearchRequest(processingSettings, requestStructure);
    }

    public String toString() {
      return "SearchRequest.SearchRequestBuilder(processingSettings=" + this.processingSettings +
          ", requestStructure=" +
          this.requestStructure + ")";
    }
  }
}
