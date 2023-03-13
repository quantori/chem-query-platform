package com.quantori.qdp.api.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Extended library with additional statistical data, like when library is created or updated, how many items
 * it contains.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode
public class Library {
  private String id;
  private String name;
  private LibraryType type;
  private Instant createdStamp;
  private Instant updatedStamp;
  private long structuresCount;
}
