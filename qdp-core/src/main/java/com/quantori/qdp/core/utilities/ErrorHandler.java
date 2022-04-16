package com.quantori.qdp.core.utilities;

import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ErrorHandler {
  List<Throwable> stopOn = List.of();
}
