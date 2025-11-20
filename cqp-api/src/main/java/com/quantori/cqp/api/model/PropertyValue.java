package com.quantori.cqp.api.model;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Container for the value of a {@link Property}. Each concrete field maps to a {@link
 * Property.PropertyType}. Only one field is expected to be non-null for a particular value.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PropertyValue {

  /** String payload for {@link Property.PropertyType#STRING} values. */
  private String stringValue;

  /** Decimal payload for {@link Property.PropertyType#DECIMAL} values. */
  private Double decimalValue;

  /** Date payload for {@link Property.PropertyType#DATE} values. */
  private LocalDate dateValue;

  /**
   * Binary content for {@link Property.PropertyType#BINARY} values. Typical use cases include
   * storing images or PDF attachments (up to 10 MB).
   */
  private byte[] binaryValue;

  /** Timestamp payload for {@link Property.PropertyType#DATE_TIME} values. */
  private Instant dateTimeValue;

  /** Ordered collection of strings for {@link Property.PropertyType#LIST} values. */
  private List<String> listValue;

  /** URL string for {@link Property.PropertyType#HYPERLINK} values. */
  private String hyperlinkValue;

  /** SMILES payload for {@link Property.PropertyType#CHEMICAL_STRUCTURE} values. */
  private String chemicalStructureValue;

  /** MOL block payload for {@link Property.PropertyType#STRUCTURE_3D} values. */
  private String structure3DValue;

  /** Sanitized HTML fragment for {@link Property.PropertyType#HTML} values. */
  private String htmlValue;
}
