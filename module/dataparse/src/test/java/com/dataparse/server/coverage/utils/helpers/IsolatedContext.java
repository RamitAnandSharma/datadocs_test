package com.dataparse.server.coverage.utils.helpers;

import com.dataparse.server.config.AppConfig;
import com.dataparse.server.config.WebMvcConfig;
import com.dataparse.server.config.WebSocketConfig;
import com.dataparse.server.service.storage.LocalFileStorage;
import com.dataparse.server.service.storage.StorageStrategyType;
import org.junit.runner.RunWith;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.util.UUID;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {AppConfig.class, WebMvcConfig.class, WebSocketConfig.class})
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class IsolatedContext {

  static {
    System.setProperty("SESSION", UUID.randomUUID().toString());
    System.setProperty("ES_INDEX_THREADS", "1");
    System.setProperty(LocalFileStorage.STORAGE_DIR_NAME, "test_storage");
    System.setProperty(AppConfig.STORAGE, StorageStrategyType.ALWAYS_GCS.name());
    System.setProperty(AppConfig.DB_RECREATE, "true");

    System.setProperty(AppConfig.DB_USERNAME, "testuser");
    System.setProperty(AppConfig.DB_PASSWORD, "testuser");

    if (System.getProperty(AppConfig.DB_NAME) == null) {
      System.setProperty(AppConfig.DB_NAME, "dataparse_test");
    }


    // for socket allow any SSL
    System.setProperty("org.eclipse.jetty.websocket.jsr356.ssl-trust-all", "true");

    // for http requests allow any SSL, especially used for socket handshake
    try {
      TrustManager[] trustAllCerts = new TrustManager[]{
          new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
              return null;
            }

            public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
            }
          }
      };

      // Install the all-trusting trust manager
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, trustAllCerts, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    } catch (Exception e) {
      System.out.println(e);
    }
  }

}
