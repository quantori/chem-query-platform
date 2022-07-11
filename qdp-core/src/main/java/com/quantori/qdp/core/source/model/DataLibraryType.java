package com.quantori.qdp.core.source.model;

public enum DataLibraryType {
  MOLECULE,
  REACTION,
  METRICS,
  ANY;

  public static DataLibraryType toDataIndexType(String type) {
    return switch (LibraryType.valueOf(type)) {
      case molecules -> DataLibraryType.MOLECULE;
      case reactions -> DataLibraryType.REACTION;
      case any -> DataLibraryType.ANY;
      case metrics -> DataLibraryType.METRICS;
    };
  }

  public static String fromDataIndexType(DataLibraryType type) {
    return switch (type) {
      case MOLECULE -> LibraryType.molecules.name();
      case REACTION -> LibraryType.reactions.name();
      case METRICS -> LibraryType.metrics.name();
      case ANY -> LibraryType.any.name();
    };
  }
}
