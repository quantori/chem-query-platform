package com.quantori.qdp.storage.api;

public interface MoleculesWriter {
  void write(Molecule molecule);

  void flush();

  void close();
}
