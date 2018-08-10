package com.dataparse.server.coverage.utils.report;

import org.junit.rules.ErrorCollector;

public class CustomErrorCollector extends ErrorCollector {
  public void verifyErrors() throws Throwable {
    verify();
  }
}
