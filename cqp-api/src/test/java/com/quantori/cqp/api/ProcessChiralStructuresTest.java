package com.quantori.cqp.api;

import static org.junit.Assert.assertTrue;

import com.epam.indigo.Indigo;
import com.quantori.cqp.api.indigo.IndigoMatcher;
import com.quantori.cqp.api.indigo.IndigoProvider;
import com.quantori.cqp.core.model.ExactParams;
import com.quantori.cqp.core.model.SubstructureParams;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ProcessChiralStructuresTest {

  private final IndigoProvider indigoProvider = new IndigoProvider(100, 5);

  @ParameterizedTest
  @ValueSource(strings = {
      "C1C=CC=C2C=CC=CC=12",
      "N1([C@@]2(SC(=N1)C(=O)C)N(N=C(c1ccc(cc1)Br)CC2)c1ccccc1)c1c(cc(cc1)Cl)Cl",
      """
          X-17641
            MACCS-II09195910443D 1   0.00000     0.00000     0
            MOE2004           3D
           38 42  0  0  0  0            999 V2000                     \s
              9.7920    2.1040    0.0000 C   0  0  0  0  0
              4.2870    7.1000    0.0000 C   0  0  0  0  0
              8.5870    7.0150    0.0000 C   0  0  0  0  0
              7.3550    2.1040    0.0000 N   0  0  0  0  0
              7.3550    4.9100    0.0000 C   0  0  0  0  0
              8.5660    4.2090    0.0000 C   0  0  0  0  0
              9.7920    6.3200    0.0000 C   0  0  0  0  0
              8.5660    8.4170    0.0000 O   0  0  0  0  0
              8.5660    0.0070    0.0000 N   0  0  0  0  0
              8.5660    2.8130    0.0000 C   0  0  2  0  0
              7.3550    9.1050    0.0000 C   0  0  0  0  0
             12.2010    2.1040    0.0000 O   0  0  0  0  0
              0.7160    3.5140    0.0000 C   0  0  0  0  0
              6.0370    0.2620    0.0000 S   0  0  0  0  0
              4.2020    4.7400    0.0000 F   0  0  0  0  0
              2.7850    4.7400    0.0000 C   0  0  0  0  0
             11.0040    0.0000    0.0000 C   0  0  0  0  0
             11.0040    4.2090    0.0000 O   0  0  0  0  0
              5.5900    3.8690    0.0000 O   0  0  0  0  0
              5.0160    9.2460    0.0000 C   0  0  0  0  0
              2.0830    5.9520    0.0000 C   0  0  0  0  0
              5.6970    7.0850    0.0000 O   0  0  0  0  0
              7.3550    6.2990    0.0000 C   0  0  0  0  0
              9.7920    4.9100    0.0000 C   0  0  0  0  0
              3.8690    8.4250    0.0000 C   0  0  0  0  0
              0.0140    2.3030    0.0000 Cl  0  0  0  0  0
              6.0370    2.5300    0.0000 C   0  0  0  0  0
              3.8050    1.4030    0.0000 C   0  0  0  0  0
              6.1570    8.3960    0.0000 C   0  0  0  0  0
              2.0830    3.5290    0.0000 C   0  0  0  0  0
              5.2010    1.4030    0.0000 C   0  0  0  0  0
              9.7920    0.6870    0.0000 C   0  0  0  0  0
             13.4130    2.8060    0.0000 C   0  0  0  0  0
             11.0040    2.8060    0.0000 C   0  0  0  0  0
              0.0000    4.7400    0.0000 C   0  0  0  0  0
              7.3550    0.6870    0.0000 C   0  0  0  0  0
              0.7160    5.9590    0.0000 C   0  0  0  0  0
              7.3550   10.5010    0.0000 O   0  0  0  0  0
            4 36  1  0  0  0
            4 27  1  0  0  0
            4 10  1  0  0  0
           36  9  2  0  0  0
           36 14  1  0  0  0
            1 10  1  0  0  0
            1 32  2  0  0  0
            1 34  1  0  0  0
           27 31  1  0  0  0
           27 19  2  0  0  0
           31 14  1  0  0  0
           31 28  2  0  0  0
            9 32  1  0  0  0
           10  6  1  0  0  0
           32 17  1  0  0  0
           28 30  1  0  0  0
           30 13  2  0  0  0
           30 16  1  0  0  0
           11 29  1  0  0  0
           11  8  1  0  0  0
           11 38  2  0  0  0
           34 18  2  0  0  0
           34 12  1  0  0  0
           29 22  1  0  0  0
           29 20  2  0  0  0
            6 24  2  0  0  0
            6  5  1  0  0  0
            8  3  1  0  0  0
           22  2  1  0  0  0
           13 26  1  0  0  0
           13 35  1  0  0  0
           16 15  1  0  0  0
           16 21  2  0  0  0
           20 25  1  0  0  0
            2 25  2  0  0  0
           24  7  1  0  0  0
            5 23  2  0  0  0
            3 23  1  0  0  0
            3  7  2  0  0  0
           12 33  1  0  0  0
           37 21  1  0  0  0
           37 35  2  0  0  0
          M  END
          """
  })
  void test_exact_match(String structure) {
    var indigoObject = new Indigo().loadMolecule(structure);
    var exactParams = ExactParams.builder().searchQuery(structure).build();
    assertTrue(new IndigoMatcher(indigoProvider).isExactMatch(indigoObject.serialize(), exactParams));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "N1([C@@](C=C(C1=O)Nc1ccc(I)cc1)(C(=O)OCC)C)c1ccc(cc1)I",
      "N1([C@@]2(SC(=N1)C(=O)C)N(N=C(c1ccc(cc1)Br)CC2)c1ccccc1)c1c(cc(cc1)Cl)Cl",
      "c1(c(c2c([nH]c1=O)ccc(c2)Cl)c1ccccc1)C1=NN(C(Nc2ccccc2)=S)[C@H](C1)c1ccc(cc1)OC",
      "c1(c(c2c([nH]c1=O)ccc(c2)Cl)c1ccccc1)C1=NN([C@H](C1)c1cc(c(cc1)OC)OC)C(=O)CCC(=O)O",
      "N1(N=C(c2c(c3c(nc2C)ccc(c3)Br)c2ccccc2)C[C@H]1c1ccc(cc1)C)C(=O)CCC(=O)O",
      "c12n(c(c3c(n1c(nn2)SCC(c1ccc(cc1)Cl)=O)sc1c3CC[C@H](C1)C(C)(C)C)=O)c1c(C)cccc1",
      "c12n([C@H](C(F)(F)F)C[C@H](N2)c2ccc(cc2)Br)ncc1C(Nc1cc(c(c(c1)OC)OC)OC)=O"
  })
  void test_sub_search(String structure) {
    var indigoObject = new Indigo().loadMolecule(structure);
    var substructureParams = SubstructureParams.builder().searchQuery(structure).heteroatoms(false).build();
    assertTrue(new IndigoMatcher(indigoProvider).isSubstructureMatch(indigoObject.serialize(), substructureParams));
  }
}
