package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.FingerPrintUtilities;
import com.quantori.qdp.storage.api.Molecule;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import org.apache.solr.client.solrj.beans.Field;

@Getter
public class SolrMoleculeDocument {
  @Field("id")
  private final String id;
  @Field("structure")
  private final String structure;
  @Field("sub")
  private final String sub;
  @Field("sim")
  private final String sim;
  @Field("exact")
  private final String exact;
  @Field("libraryId")
  private final String libraryId;
  @Field("name")
  private final String name;
  @Field("inchi")
  private final String inchi;
  private final Map<String, String> molproperties;// properties
  @Field("*")
  private final Map<String, Object> parsedProperties;

  @SuppressWarnings("unused")
  public SolrMoleculeDocument() {
    this.id = null;
    this.structure = null;
    this.sub = null;
    this.sim = null;
    this.molproperties = null;
    this.exact = null;
    this.parsedProperties = null;
    this.inchi = null;
    this.name = null;
    this.libraryId = null;
  }

  public SolrMoleculeDocument(Molecule molecule, String libraryId) {
    this.id = molecule.getId();
    this.structure = molecule.getStructure();
    this.exact = this.structure == null ? null : FingerPrintUtilities.exactHash(molecule.getExact());
    this.sub = this.structure == null ? null : FingerPrintUtilities.substructureHash(molecule.getSub());
    this.sim = this.structure == null ? null : FingerPrintUtilities.similarityHash(molecule.getSim());
    this.molproperties = molecule.getMolProperties();
    this.parsedProperties = parse(molecule.getMolProperties());
    this.inchi = molecule.getInchi();
    this.name = molecule.getName();
    this.libraryId = libraryId;
  }

  public static SolrMoleculeDocument withGeneratedId(Molecule molecule, String libraryId) {
    molecule.setId(UUID.randomUUID().toString());
    return new SolrMoleculeDocument(molecule, libraryId);
  }

  private static Object parse(String val) {
    try {
      return Double.parseDouble(val);
    } catch (NumberFormatException ex) {
      return val;
    }
  }

  private Map<String, Object> parse(Map<String, String> molProperties) {
    Map<String, Object> parsedProps = new HashMap<>();
    if (molProperties == null) {
      return parsedProps;
    }
    molProperties.forEach((key, value) -> parsedProps.put(key, parse(value)));
    return parsedProps;
  }
}

