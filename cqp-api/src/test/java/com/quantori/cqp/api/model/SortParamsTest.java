package com.quantori.cqp.api.model;

import org.assertj.core.data.Index;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class SortParamsTest {

  @Test
  void testEmptySortParams() {
    org.assertj.core.api.Assertions.assertThat(SortParams.empty().sortList()).isEmpty();
  }

  @Test
  void testCreateSortParameterWithNullListImpossible() {
    SortParams empty = SortParams.empty();
    //ensure unable to create invalid project
    Assertions.assertAll(
        () -> Assertions.assertThrows(NullPointerException.class, () -> SortParams.ofSortList(null)),
        () -> Assertions.assertThrows(NullPointerException.class, () -> new SortParams(null)),
        //() -> Assertions.assertThrows(NullPointerException.class, () -> SortParams.of(null)),
        () -> Assertions.assertThrows(NullPointerException.class, () -> empty.addFirstSort(null)),
        () -> Assertions.assertThrows(NullPointerException.class, () -> empty.addLastSort(null))
    );
  }

  @Test
  void testCreateSortParameterOfSingleSortRule() {
    var sort = SortParams.Order.ASC.general("MyField");
    var params = SortParams.of(sort);
    Assertions.assertAll(
        () -> org.assertj.core.api.Assertions.assertThat(params.sortList()).hasSize(1),
        () -> org.assertj.core.api.Assertions.assertThat(params.sortList()).containsExactly(sort)
    );
  }


  @Test
  void testCreateSortParameterThisSingleSortRuleNullableImpossible() {
    var sort = SortParams.Order.ASC.general("MyField");
    SortParams params = SortParams.empty().addLastSort(sort);
    Assertions.assertAll(
        () -> org.assertj.core.api.Assertions.assertThat(params.sortList()).hasSize(1),
        () -> org.assertj.core.api.Assertions.assertThat(params.sortList()).containsExactly(sort)
    );
  }

  @Test
  void testCreateSortUsingVarags() {
    var sort0 = SortParams.Order.ASC.general("MyField");
    var sort1 = SortParams.Order.DESC.general("Fila");
    SortParams params = SortParams.of(sort0, sort1);
    Assertions.assertAll(
        () -> Assertions.assertEquals(2, params.sortList().size()),
        () -> org.assertj.core.api.Assertions.assertThat(params.sortList()).containsExactly(sort0, sort1)
    );
  }


  @Test
  void testEnsureEmptyListSpareObjectAllocation() {
    var params = SortParams.ofSortList(List.of());
    org.assertj.core.api.Assertions.assertThat(params.sortList()).isEmpty();
  }

  @Test
  void testAddFirstAddLast() {
    var sort0 = SortParams.Order.ASC.general("MyField");
    var sort1 = SortParams.Order.DESC.general("Fila");
    var sort2 = SortParams.Order.ASC.nested("queu-qiei");
    SortParams params = SortParams.ofSortList(List.of(sort0, sort1));
    var addLastParams = params.addLastSort(sort2);
    var addFirstParams = params.addFirstSort(sort2);
    Assertions.assertAll(
        () -> org.assertj.core.api.Assertions.assertThat(addLastParams.sortList()).hasSize(3),
        () -> org.assertj.core.api.Assertions.assertThat(addLastParams.sortList()).contains(sort2, Index.atIndex(2)),
        () -> org.assertj.core.api.Assertions.assertThat(addFirstParams.sortList()).hasSize(3),
        () -> org.assertj.core.api.Assertions.assertThat(addFirstParams.sortList()).contains(sort2, Index.atIndex(0)),
        //ensure the original sort parameters object wasn't modified
        () -> org.assertj.core.api.Assertions.assertThat(params.sortList())
            .containsExactlyElementsOf(List.of(sort0, sort1))
    );
  }

  @Test
  void testAddLastAddFisrtAsSortParameters() {
    var sort0 = SortParams.Order.ASC.general("MyField");
    var sort1 = SortParams.Order.DESC.general("Fila");
    var sort2Order = SortParams.Order.ASC;
    var sort2Type = SortParams.Type.NESTED;
    var sort2FieldName = "queu-qiei";
    var sort2 = new SortParams.Sort(sort2FieldName, sort2Order, sort2Type);
    SortParams params = SortParams.ofSortList(List.of(sort0, sort1));
    var addLastParams = params.addLastSort(sort2FieldName, sort2Order, sort2Type);
    var addFirstParams = params.addFirstSort(sort2FieldName, sort2Order, sort2Type);
    Assertions.assertAll(
        () -> org.assertj.core.api.Assertions.assertThat(addLastParams.sortList()).hasSize(3),
        () -> org.assertj.core.api.Assertions.assertThat(addLastParams.sortList()).contains(sort2, Index.atIndex(2)),
        () -> org.assertj.core.api.Assertions.assertThat(addFirstParams.sortList()).hasSize(3),
        () -> org.assertj.core.api.Assertions.assertThat(addFirstParams.sortList()).contains(sort2, Index.atIndex(0)),
        //ensure the original sort parameters object wasn't modified
        () -> org.assertj.core.api.Assertions.assertThat(params.sortList())
            .containsExactlyElementsOf(List.of(sort0, sort1))
    );
  }
}