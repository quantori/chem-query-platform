package com.quantori.qdp.core.source;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.quantori.qdp.core.model.StorageUploadItem;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;
import java.util.Map;

/**
 * Generic definition of molecule structure.
 */
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = TestMolecule.class)
public class TestMolecule extends BasicMolecule implements StorageUploadItem {
    private byte[] sim;
    private String exactHash;
    private Map<String, String> molProperties;
    private Long customOrder;
    private Instant createdStamp;
    private Instant updatedStamp;

    public Map<String, String> getMolProperties() {
        if (molProperties == null) {
            return Map.of();
        }
        return molProperties;
    }
}
