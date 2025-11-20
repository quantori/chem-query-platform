package com.quantori.cqp.api.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import java.util.EnumSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class PropertyTypeTest {

  @Test
  void shouldContainExtendedTypes() {
    EnumSet<Property.PropertyType> types = EnumSet.allOf(Property.PropertyType.class);

    assertThat(types)
        .contains(
            Property.PropertyType.BINARY,
            Property.PropertyType.DATE_TIME,
            Property.PropertyType.LIST,
            Property.PropertyType.HYPERLINK,
            Property.PropertyType.CHEMICAL_STRUCTURE,
            Property.PropertyType.STRUCTURE_3D,
            Property.PropertyType.HTML);
  }

  @Test
  void shouldAllowPropertyValueAccessors() {
    byte[] binary = new byte[] {1, 2, 3};
    Instant timestamp = Instant.parse("2024-01-01T10:15:30Z");
    List<String> orderedList = List.of("first", "second");
    LocalDate date = LocalDate.of(2024, 2, 29);

    PropertyValue value =
        PropertyValue.builder()
            .stringValue("test")
            .decimalValue(12.34d)
            .dateValue(date)
            .binaryValue(binary)
            .dateTimeValue(timestamp)
            .listValue(orderedList)
            .hyperlinkValue("https://example.com")
            .chemicalStructureValue("CCO")
            .structure3DValue("3D-MOL-DATA")
            .htmlValue("<p>html</p>")
            .build();

    assertThat(value.getStringValue()).isEqualTo("test");
    assertThat(value.getDecimalValue()).isEqualTo(12.34d);
    assertThat(value.getDateValue()).isEqualTo(date);
    assertThat(value.getBinaryValue()).containsExactly(binary);
    assertThat(value.getDateTimeValue()).isEqualTo(timestamp);
    assertThat(value.getListValue()).containsExactlyElementsOf(orderedList);
    assertThat(value.getHyperlinkValue()).isEqualTo("https://example.com");
    assertThat(value.getChemicalStructureValue()).isEqualTo("CCO");
    assertThat(value.getStructure3DValue()).isEqualTo("3D-MOL-DATA");
    assertThat(value.getHtmlValue()).isEqualTo("<p>html</p>");
  }
}
