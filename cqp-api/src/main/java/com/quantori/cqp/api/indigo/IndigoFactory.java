package com.quantori.cqp.api.indigo;

import com.epam.indigo.Indigo;
import com.sun.jna.Platform;
import lombok.experimental.UtilityClass;

/**
 * Class for creating new Indigo and IndigoInchi objects.
 */
@UtilityClass
public final class IndigoFactory {

    public static Indigo createNewIndigo() {

        final Indigo indigo;
        if (Platform.isMac()) {
            String previousVersion = System.setProperty("os.version", "10.7");
            indigo = new Indigo();
            System.setProperty("os.version", previousVersion);
        } else {
            indigo = new Indigo();
        }

        indigo.setOption("ignore-stereochemistry-errors", true);

        return indigo;
    }

    public static IndigoInchi createNewIndigoInchi() {

        return new IndigoInchi(createNewIndigo());
    }
}
