package com.quantori.cqp.api.model.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchError {
  private ErrorType type;
  private String storage;
  private List<String> libraryIds;
  private String message;
}
