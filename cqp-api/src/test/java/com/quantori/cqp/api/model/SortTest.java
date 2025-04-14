package com.quantori.cqp.api.model;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class SortTest {

  @Test
  void testInvert() {
    var sort = SortParams.Order.ASC.general("fine-pit");
    var inverted = sort.invertOrder();
    var secondTimesInverted = inverted.invertOrder();
    Assertions.assertThat(sort.order()).isSameAs(SortParams.Order.ASC);
    Assertions.assertThat(inverted.order()).isSameAs(SortParams.Order.DESC);
    Assertions.assertThat(secondTimesInverted).isEqualTo(sort);
  }

  @Test
  void testCreateSortParamsFromThisSort() {
    var sort = SortParams.Order.ASC.general("fine-pit");
    var actual = sort.asSortParams();
    var expected = new SortParams(List.of(sort));
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
  }

}
