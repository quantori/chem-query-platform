package com.quantori.cqp.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class Property {
  private String name;
  private PropertyType type;
  private int position;
  private boolean hidden;
  private boolean deleted;

  public Property(String name, PropertyType type) {
    this.name = name;
    this.type = type;
  }

  /**
   * Defines the supported property kinds in Chem Query Platform. Existing values must remain
   * unchanged to keep backward compatibility with previously persisted data.
   */
  public enum PropertyType {
    STRING,
    DECIMAL,
    DATE,
    /**
     * Binary payload for storing images, PDFs or other small files. Intended size limit is 10 MB per
     * value and the binary content is transferred as byte arrays.
     */
    BINARY,
    /**
     * Timestamp value with timezone information preserved. Values are serialized as ISO-8601
     * instants (e.g. {@code 2025-01-15T10:30:00Z}).
     */
    DATE_TIME,
    /**
     * Ordered collection of string values. This allows preserving the order in which the user
     * provided the values (e.g. {@code ["first", "second"]}).
     */
    LIST,
    /**
     * Hyperlink that stores HTTP/HTTPS (and similar) URL references. Validation is applied on
     * backend layers while the enum only marks the data type.
     */
    HYPERLINK,
    /**
     * Chemical structure serialized as a SMILES string. Consumers rely on Indigo toolkit for
     * validation.
     */
    CHEMICAL_STRUCTURE,
    /**
     * Three dimensional molecular structure stored as MOL text block. This is typically used for 3D
     * renderings and cannot be exported to legacy SDF files.
     */
    STRUCTURE_3D,
    /**
     * Sanitized HTML fragment used for rich text property rendering. Scripts and unsafe tags are
     * expected to be removed before persisting.
     */
    HTML
  }
}
