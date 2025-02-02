package com.quantori.qdp.core.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.validation.constraints.NotNull;

/**
 * Sort parameters. Immutable object, all methods are safe.
 */
@SuppressWarnings("unused")
public record SortParams(List<Sort> sortList) {

  private static final SortParams EMPTY = new SortParams(List.of());

  /**
   * Constructs a {@code SortParams} object with the specified list of sort rules.
   * <p>
   * If the list contains multiple sort rules for the same field, only the first occurrence of the sort rule
   * for that field in the list is applied, and subsequent sort rules for the same field are ignored.
   * It is up to client to make sure the sort rules provided
   *
   */
  public SortParams(@NotNull List<Sort> sortList) {
    Objects.requireNonNull(sortList);
    this.sortList = List.copyOf(sortList);
  }

  /**
   * Empty list of sorting parameters.
   *
   * @return empty list sort parameters
   */
  @NotNull
  public static SortParams empty() {
    return EMPTY;
  }

  /**
   * Create sorting parameters of a single sorting parameter.
   *
   * @param sort rule
   * @return sort parameters
   */
  public static @NotNull SortParams of(@NotNull Sort sort) {
    Objects.requireNonNull(sort);
    return ofSortList(List.of(sort));
  }

  /**
   * Create the list of sort parameters.
   *
   * @param sorts rules
   * @return list of sort parameters
   */
  public static SortParams of(Sort... sorts) {
    return ofSortList(List.of(sorts));
  }

  /**
   * Create the list of sort parameters.
   * <p>
   * If the list contains multiple sort rules for the same field, only the first occurrence of the sort rule
   * for that field in the list is applied, and subsequent sort rules for the same field are ignored.
   * It is up to client to make sure the sort rules provided
   *
   * @param sortList of parameters
   * @return list of sorting parameters
   */
  public static SortParams ofSortList(@NotNull List<Sort> sortList) {
    Objects.requireNonNull(sortList);
    if (sortList.isEmpty()) {
      return EMPTY;
    }
    return new SortParams(sortList);
  }

  /**
   * Create new sort parameters with new sort rule added at the end of sort list.
   *
   * @param sort parameter
   * @return new sort parameters
   */
  public SortParams addLastSort(@NotNull Sort sort) {
    Objects.requireNonNull(sort);
    var list = new ArrayList<>(sortList);
    list.add(sort);
    return new SortParams(list);
  }

  /**
   * Create new sort parameters with new sort rule added at the end of sort list.
   *
   * @param fieldName {@link Sort#fieldName}
   * @param order     {@link Sort#order}
   * @param type      {@link Sort#type}
   * @return new sort parameters
   */
  public SortParams addLastSort(String fieldName, Order order, Type type) {
    return addLastSort(new Sort(fieldName, order, type));
  }

  /**
   * Create new sort parameters with new sort rule added at the end of sort list.
   *
   * @param sort parameter
   * @return new sort parameters
   */
  public SortParams addFirstSort(@NotNull Sort sort) {
    Objects.requireNonNull(sort);
    List<Sort> list = new ArrayList<>();
    list.add(sort);
    list.addAll(this.sortList());
    return SortParams.ofSortList(list);
  }

  /**
   * Create new sort parameters with new sort rule added at the end of sort list.
   *
   * @param fieldName {@link Sort#fieldName}
   * @param order     {@link Sort#order}
   * @param type      {@link Sort#type}
   * @return new sort parameters
   */
  public SortParams addFirstSort(String fieldName, Order order, Type type) {
    return addFirstSort(new Sort(fieldName, order, type));
  }


  public enum Type {GENERAL, NESTED}

  public enum Order {
    ASC {
      @Override
      public Order invert() {
        return DESC;
      }
    }, DESC {
      @Override
      public Order invert() {
        return ASC;
      }
    };

    /**
     * Create sort rule of general field.
     *
     * @param fieldName to sort by
     */
    public Sort general(String fieldName) {
      return new Sort(fieldName, this, Type.GENERAL);
    }

    /**
     * Create sort rule of nested field.
     *
     * @param fieldName to sort by
     */
    public Sort nested(String fieldName) {
      return new Sort(fieldName, this, Type.NESTED);
    }

    private Sort of(String fieldName, Type type) {
      return new Sort(fieldName, this, type);
    }

    /**
     * Invert order <i>desc</i> to <i>asc</i> and <i>asc</i> to <i>desc</i>.
     */
    public abstract Order invert();
  }

  public record Sort(String fieldName, Order order, @NotNull Type type) {

    /**
     * Invert sort order of field.
     *
     * @return inverted order of current sort
     */
    public Sort invertOrder() {
      return new Sort(this.fieldName, order.invert(), type);
    }

    /**
     * Create a sort parameters list with single sort rule from this sort.
     *
     * @return sort parameters list
     */
    public SortParams asSortParams() {
      return SortParams.of(this);
    }
  }
}
