package com.quantori.cqp.api;

import com.quantori.cqp.api.indigo.IndigoMatcher;
import com.quantori.cqp.api.indigo.IndigoProvider;
import com.quantori.cqp.api.model.SubstructureParams;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

class ProcessStructureTest {

  private static Stream<Arguments> molecules() {
    return Stream.of(
        Arguments.of("C1C=CC=C2CCC=12>>C1CCC1",
            "[NH2:1][C:2]1=[CH:3][CH:4]=[C:5]([C:8]2=[CH:9][N:10]=[C:11]([C:13]3([CH2:14][CH2:15][CH2:16]3)[O:17]"
                + "[CH2:18][C:19](=[O:20])[O:21]C)[S:12]2)[CH:6]=[CH:7]1.[CH:23]1([CH2:24][C:25]2=[C:26]1[CH:27]="
                + "[CH:28][CH:29]=[CH:30]2)[C:31](=[O:32])O.CN1CCOCC1.O.ON1N=NC2=C1C=CC=C2.Cl.C(C)N=C=NCCCN(C)C>CN(C=O)"
                + "C>[C:25]12=[CH:30][CH:29]=[CH:28][CH:27]=[C:26]1[CH:23]([C:31](=[O:32])[NH:1][C:2]1=[CH:3][CH:4]"
                + "=[C:5]([C:8]3=[CH:9][N:10]=[C:11]([C:13]4([CH2:14][CH2:15][CH2:16]4)[O:17][CH2:18][C:19](=[O:20])"
                + "[OH:21])[S:12]3)[CH:6]=[CH:7]1)[CH2:24]2 |f:3.4,5.6|"),

        Arguments.of("C1C=CC=C2CCC=12>>C1CCC1",
            "[NH2:1][C:2]1=[CH:3][CH:4]=[C:5]([C:8]2=[CH:9][N:10]=[C:11]([C:13]3([CH2:14][CH2:15][CH2:16]3)[O:17]"
                + "[CH2:18][C:19](=[O:20])[O:21]C)[S:12]2)[CH:6]=[CH:7]1.[CH:23]1([CH2:24][C:25]2=[C:26]1[CH:27]="
                + "[CH:28][CH:29]=[CH:30]2)[C:31](=[O:32])O.CN1CCOCC1.O.ON1N=NC2=C1C=CC=C2.Cl.C(C)N=C=NCCCN(C)C>CN(C=O)"
                + "C>[C:25]12=[CH:30][CH:29]=[CH:28][CH:27]=[C:26]1[CH:23]([C:31](=[O:32])[NH:1][C:2]1=[CH:3][CH:4]="
                + "[C:5]([C:8]3=[CH:9][N:10]=[C:11]([C:13]4([CH2:14][CH2:15][CH2:16]4)[O:17][CH2:18][C:19](=[O:20])"
                + "[OH:21])[S:12]3)[CH:6]=[CH:7]1)[CH2:24]2 |f:3.4,5.6|"),

        Arguments.of("C1C=CC=C2CCC=12>>C1CCC1",
            "[NH2:1][C:2]1=[CH:3][CH:4]=[C:5]([C:8]2=[CH:9][N:10]=[C:11]([C:13]3([CH2:14][CH2:15][CH2:16]3)[O:17]"
                + "[CH2:18][C:19](=[O:20])[O:21]C)[S:12]2)[CH:6]=[CH:7]1.[CH:23]1([CH2:24][C:25]2=[C:26]1[CH:27]="
                + "[CH:28][CH:29]=[CH:30]2)[C:31](=[O:32])O.CN1CCOCC1.O.ON1N=NC2=C1C=CC=C2.Cl.C(C)N=C=NCCCN(C)C>CN(C=O)"
                + "C>[C:25]12=[CH:30][CH:29]=[CH:28][CH:27]=[C:26]1[CH:23]([C:31](=[O:32])[NH:1][C:2]1=[CH:3][CH:4]="
                + "[C:5]([C:8]3=[CH:9][N:10]=[C:11]([C:13]4([CH2:14][CH2:15][CH2:16]4)[O:17][CH2:18][C:19](=[O:20])"
                + "[OH:21])[S:12]3)[CH:6]=[CH:7]1)[CH2:24]2 |f:3.4,5.6|\n"),

        Arguments.of("C1C=CC=[C:3]2[CH2:8][CH2:7][C:2]=12>>[CH2:2]1[CH2:7][CH2:8][CH2:3]1",
            "[NH2:1][C:2]1=[CH:3][CH:4]=[C:5]([C:8]2=[CH:9][N:10]=[C:11]([C:13]3([CH2:14][CH2:15][CH2:16]3)[O:17]"
                + "[CH2:18][C:19](=[O:20])[O:21]C)[S:12]2)[CH:6]=[CH:7]1.[CH:23]1([CH2:24][C:25]2=[C:26]1[CH:27]="
                + "[CH:28][CH:29]=[CH:30]2)[C:31](=[O:32])O.CN1CCOCC1.O.ON1N=NC2=C1C=CC=C2.Cl.C(C)N=C=NCCCN(C)C>CN(C=O)"
                + "C>[C:25]12=[CH:30][CH:29]=[CH:28][CH:27]=[C:26]1[CH:23]([C:31](=[O:32])[NH:1][C:2]1=[CH:3][CH:4]="
                + "[C:5]([C:8]3=[CH:9][N:10]=[C:11]([C:13]4([CH2:14][CH2:15][CH2:16]4)[O:17][CH2:18][C:19](=[O:20])"
                + "[OH:21])[S:12]3)[CH:6]=[CH:7]1)[CH2:24]2 |f:3.4,5.6|\n"),

        Arguments.of("C1C=CC=[C:3]2[CH2:8][CH2:7][C:2]=12>>[CH2:2]1[CH2:7][CH2:8][CH2:3]1",
            "[NH2:1][C:2]1=[CH:3][CH:4]=[C:5]([C:8]2=[CH:9][N:10]=[C:11]([C:13]3([CH2:14][CH2:15][CH2:16]3)[O:17]"
                + "[CH2:18][C:19](=[O:20])[O:21]C)[S:12]2)[CH:6]=[CH:7]1.[CH:23]1([CH2:24][C:25]2=[C:26]1[CH:27]="
                + "[CH:28][CH:29]=[CH:30]2)[C:31](=[O:32])O.CN1CCOCC1.O.ON1N=NC2=C1C=CC=C2.Cl.C(C)N=C=NCCCN(C)C>CN(C=O)"
                + "C>[C:25]12=[CH:30][CH:29]=[CH:28][CH:27]=[C:26]1[CH:23]([C:31](=[O:32])[NH:1][C:2]1=[CH:3][CH:4]="
                + "[C:5]([C:8]3=[CH:9][N:10]=[C:11]([C:13]4([CH2:14][CH2:15][CH2:16]4)[O:17][CH2:18][C:19](=[O:20])"
                + "[OH:21])[S:12]3)[CH:6]=[CH:7]1)[CH2:24]2 |f:3.4,5.6|")
    );
  }

  @ParameterizedTest
  @MethodSource("molecules")
  void test(String source, String target) {
    var substructureParams = SubstructureParams.builder().searchQuery(source).heteroatoms(false).build();
    Assertions.assertTrue(new IndigoMatcher(new IndigoProvider(100, 5)).isReactionSubstructureMatch(target, substructureParams));
  }
}
