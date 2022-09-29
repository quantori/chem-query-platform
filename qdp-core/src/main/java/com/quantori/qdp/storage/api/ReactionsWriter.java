package com.quantori.qdp.storage.api;

public interface ReactionsWriter {
  void write(ReactionUploadDocument uploadDocument);

  void flush();

  void close();
}
