package com.quantori.qdp.api.model;

import java.util.List;

public record SortParams(List<Sort> sortList) {
    public static SortParams ofSortList(List<Sort> sortList) {
        return new SortParams(sortList);
    }

    public enum Type {GENERAL, NESTED}

    public enum Order {ASC, DESC}

    public record Sort(String fieldName, Order order, Type type) {
    }
}
