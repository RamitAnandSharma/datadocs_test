package com.dataparse.server;

import com.dataparse.server.config.AppConfig;
import com.dataparse.server.config.WebMvcConfig;
import com.dataparse.server.config.WebSocketConfig;
import com.dataparse.server.service.storage.StorageStrategyType;
import org.junit.runner.RunWith;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {AppConfig.class, WebMvcConfig.class, WebSocketConfig.class})
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class IsolatedContextTest {

  static {
    System.setProperty(AppConfig.DB_RECREATE, "true");
    if (System.getProperty(AppConfig.DB_NAME) == null) {
      System.setProperty(AppConfig.DB_NAME, "dataparse_test");
    }
    
    System.setProperty(AppConfig.DB_USERNAME, "testuser");
    System.setProperty(AppConfig.DB_PASSWORD, "testuser");
    System.setProperty(AppConfig.STORAGE, StorageStrategyType.ALWAYS_GCS.name());
  }

}
