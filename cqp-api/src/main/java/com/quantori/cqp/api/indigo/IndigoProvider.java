package com.quantori.cqp.api.indigo;

import com.epam.indigo.Indigo;

public class IndigoProvider extends ObjectProvider<Indigo> {

  public IndigoProvider(int maximumPoolSize, int timeoutInSeconds) {
    super(new IndigoPool(maximumPoolSize), timeoutInSeconds);
  }
}
