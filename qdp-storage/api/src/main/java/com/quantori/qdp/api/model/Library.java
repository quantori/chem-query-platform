package com.quantori.qdp.api.model;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
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
  private Map<String, Object> serviceData = new HashMap<>();
  private Instant createdStamp;
  private Instant updatedStamp;
  private long structuresCount;
  private List<String> properties = List.of();
}
