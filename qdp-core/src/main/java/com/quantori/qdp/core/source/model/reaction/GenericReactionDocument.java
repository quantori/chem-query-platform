package com.quantori.qdp.core.source.model.reaction;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class GenericReactionDocument extends AbstractReactionDocument {
  private byte[] sub;
}