package com.quantori.qdp.core.source.model.reaction;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Reaction {

  private String id;

  private String reactionSmiles;

  private String reactionId;

  private String source;

  private String description;

  private String amount;

  private String subHash;
}
