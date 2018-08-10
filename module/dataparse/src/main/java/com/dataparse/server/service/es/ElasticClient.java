package com.dataparse.server.service.es;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.stereotype.Service;

import com.dataparse.server.util.SystemUtils;
import com.google.gson.GsonBuilder;

import io.searchbox.client.AbstractJestClient;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.http.JestHttpClient;

@Service
public class ElasticClient {

  private static final String ELASTIC_URL = "ELASTIC_URL";
  private static final String ELASTIC_AUTH = "ES_AUTH";
  private static final String ELASTIC_USERNAME = "ES_USERNAME";
  private static final String ELASTIC_PASSWORD = "ES_PASSWORD";

  private JestClient client;

  private String getElasticUrl(){
    return SystemUtils.getProperty(ELASTIC_URL, "http://localhost:9200");
  }

  private Boolean getElasticAuthEnabled() {
    return SystemUtils.getProperty(ELASTIC_AUTH, false);
  }

  private String getElasticUsername() {
    return SystemUtils.getProperty(ELASTIC_USERNAME, "elastic");
  }

  private String getElasticPassword() {
    return SystemUtils.getProperty(ELASTIC_PASSWORD, "changeme");
  }

  @PostConstruct
  protected void init() throws Exception {
    JestClientFactory factory = new JestClientFactory();
    HttpClientConfig.Builder configBuilder = new HttpClientConfig
        .Builder(getElasticUrl())
        .connTimeout(60000)
        .readTimeout(60000)
        .multiThreaded(true)
        .maxTotalConnection(75)
        .defaultMaxTotalConnectionPerRoute(75);
    if(getElasticAuthEnabled()){
      configBuilder.defaultCredentials(getElasticUsername(), getElasticPassword());
    }
    factory.setHttpClientConfig(configBuilder.build());
    client = factory.getObject();
    ((JestHttpClient) client).setGson(new GsonBuilder()
        .setDateFormat(AbstractJestClient.ELASTIC_SEARCH_DATE_FORMAT)
        .serializeSpecialFloatingPointValues()
        .create());
  }

  @PreDestroy
  protected void destroy(){
    client.shutdownClient();
  }

  public JestClient getClient(){
    return client;
  }

}
