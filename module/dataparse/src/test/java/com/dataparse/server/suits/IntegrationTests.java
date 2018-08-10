package com.dataparse.server.suits;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.dataparse.server.ingest.BaseIngestTests;
import com.dataparse.server.tool.ServerTool;

@RunWith(Suite.class)
@Suite.SuiteClasses(BaseIngestTests.class)
public class IntegrationTests {

  @BeforeClass
  public static void beforeClass() {
    String relPath = 
      IntegrationTests.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    String baseDir = new File(relPath+"../../../..").getAbsolutePath();
    System.out.println("\nbaseDir = " + baseDir + "\n");
    System.setProperty("app.webapp.dir", baseDir + "/release/app/src/main/webapp");
    ServerTool.cleanAllWithDefaultConfig();
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
  }

}
