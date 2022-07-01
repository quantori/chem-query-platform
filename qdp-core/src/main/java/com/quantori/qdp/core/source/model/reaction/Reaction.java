package com.quantori.qdp.core.source.model.reaction;

import com.quantori.qdp.core.source.model.molecule.ReactionMolecule;
import java.util.List;
import javax.validation.constraints.NotNull;
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

  @NotNull
  GenericReactionDocument reactionDocument;
  @NotNull
  List<ReactionMolecule> participantEntities;
}
