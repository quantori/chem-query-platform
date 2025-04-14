package com.quantori.cqp.api.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OrderTest {

  @Test
  void testInvert() {
    Assertions.assertAll(
        () -> Assertions.assertSame(SortParams.Order.DESC, SortParams.Order.ASC.invert()),
        () -> Assertions.assertSame(SortParams.Order.ASC, SortParams.Order.DESC.invert())
    );
  }

  @Test
  void testCreateObjectUsingAscDesc() {
    Assertions.assertAll(
        () -> Assertions.assertEquals(
            new SortParams.Sort("my-field", SortParams.Order.ASC, SortParams.Type.GENERAL),
            SortParams.Order.ASC.general("my-field")
        ),

        () -> Assertions.assertEquals(
            new SortParams.Sort("my-field", SortParams.Order.DESC, SortParams.Type.GENERAL),
            SortParams.Order.DESC.general("my-field")
        ),

        () -> Assertions.assertNotEquals(
            new SortParams.Sort("no-filedo", SortParams.Order.ASC, SortParams.Type.NESTED),
            SortParams.Order.ASC.nested("my-field")
        ),

        () -> Assertions.assertNotEquals(
            new SortParams.Sort("no-filedo", SortParams.Order.DESC, SortParams.Type.NESTED),
            SortParams.Order.DESC.nested("my-field")
        )
    );

  }
}
