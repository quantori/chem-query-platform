package com.quantori.qdp.core.source;

import com.quantori.qdp.api.model.core.ProcessingSettings;
import com.quantori.qdp.api.model.core.RequestStructure;

public class SearchRequest {
  private final ProcessingSettings processingSettings;
  private final RequestStructure<TestSearchItem, TestStorageItem> requestStructure;

  SearchRequest(ProcessingSettings processingSettings,
                RequestStructure<TestSearchItem, TestStorageItem> requestStructure) {
    this.processingSettings = processingSettings;
    this.requestStructure = requestStructure;
  }

  public static SearchRequestBuilder builder() {
    return new SearchRequestBuilder();
  }

  public ProcessingSettings getProcessingSettings() {
    return this.processingSettings;
  }

  public RequestStructure<TestSearchItem, TestStorageItem> getRequestStructure() {
    return this.requestStructure;
  }

  public static class SearchRequestBuilder {
    private ProcessingSettings processingSettings;
    private RequestStructure<TestSearchItem, TestStorageItem> requestStructure;

    SearchRequestBuilder() {
    }

    public SearchRequestBuilder processingSettings(ProcessingSettings processingSettings) {
      this.processingSettings = processingSettings;
      return this;
    }

    public SearchRequestBuilder requestStructure(RequestStructure<TestSearchItem, TestStorageItem> requestStructure) {
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
