package com.quantori.cqp.api.indigo;

import com.epam.indigo.Indigo;
import lombok.Getter;

/**
 * IndigoInchi extended with a sid to be available for the IndigoInchiPool.
 */
@Getter
public class IndigoInchi extends com.epam.indigo.IndigoInchi {

    private final long sid;

    public IndigoInchi(Indigo indigo) {
        super(indigo);
        this.sid = indigo.getSid();
    }
}
