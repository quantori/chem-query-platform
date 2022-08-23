package com.quantori.qdp.core.source.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StorageError {
  private String storage;
  private List<String> libraryIds;
  private String message;
}
