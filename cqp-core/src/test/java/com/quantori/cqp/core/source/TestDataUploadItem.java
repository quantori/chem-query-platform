package com.quantori.cqp.core.source;

import com.quantori.cqp.core.model.DataUploadItem;
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
