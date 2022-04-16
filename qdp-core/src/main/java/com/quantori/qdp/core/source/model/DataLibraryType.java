package com.quantori.qdp.core.source.model;

public enum DataLibraryType {
  MOLECULE,
  REACTION,
  METRICS,
  ANY;

  public static DataLibraryType toDataIndexType(String type) {
    switch (LibraryType.valueOf(type)) {
      case molecules:
        return DataLibraryType.MOLECULE;
      case reactions:
        return DataLibraryType.REACTION;
      case any:
        return DataLibraryType.ANY;
      case metrics:
        return DataLibraryType.METRICS;
    }
    throw new IllegalArgumentException("Failed to convert " + type + " to DataLibraryType");
  }

  public static String fromDataIndexType(DataLibraryType type) {
    switch (type) {
      case MOLECULE:
        return LibraryType.molecules.name();
      case REACTION:
        return LibraryType.reactions.name();
      case METRICS:
        return LibraryType.metrics.name();
      case ANY:
        return LibraryType.any.name();
    }
    throw new IllegalArgumentException("Failed to convert " + type + " to LibraryType");
  }
}
