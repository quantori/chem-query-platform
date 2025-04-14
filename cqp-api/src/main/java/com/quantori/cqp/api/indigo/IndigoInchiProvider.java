package com.quantori.cqp.api.indigo;

public class IndigoInchiProvider extends ObjectProvider<IndigoInchi> {

  public IndigoInchiProvider(int maximumPoolSize, int timeoutInSeconds) {
    super(new IndigoInchiPool(maximumPoolSize), timeoutInSeconds);
  }
}
