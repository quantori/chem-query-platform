package com.quantori.qdp.core.source;

import com.quantori.qdp.core.model.DataUploadItem;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class TestDataUploadItem implements DataUploadItem {
  private String id;
}
