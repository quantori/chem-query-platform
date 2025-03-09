package com.quantori.qdp.core.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchError {
  private ErrorType type;
  private String storage;
  private List<String> libraryIds;
  private String message;
}
