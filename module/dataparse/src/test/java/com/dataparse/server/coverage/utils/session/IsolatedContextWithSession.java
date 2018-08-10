package com.dataparse.server.coverage.utils.session;

import com.dataparse.server.controllers.api.flow.FlowExecutionTasksResponse;
import com.dataparse.server.controllers.api.flow.GetStateRequest;
import com.dataparse.server.coverage.utils.report.ReportedTest;
import com.dataparse.server.service.tasks.TaskInfo;
import com.dataparse.server.util.SystemUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Assert;
import org.junit.Before;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;


@Slf4j
public abstract class IsolatedContextWithSession extends ReportedTest {

  private String endpoint = SystemUtils.getProperty("PROTOCOL", "http") + "://"
      + SystemUtils.getProperty("ENDPOINT", "localhost:9100");

  @Getter
  protected ObjectMapper mapper = new ObjectMapper();
  private RestTemplate restTemplate = getRestTemplate();

  @Getter
  private List<String> sessionCookies;

  @Getter
  protected long userId;
  private Boolean silentRequestsMode = true;

  public IsolatedContextWithSession() {
    this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  // =================================================================================================================

  @SuppressWarnings("unchecked")
  protected <ReqType, ResType> ResType sendPost(String url, ReqType request, Class<ResType> responseType) throws IOException {
    String requestJson = mapper.writeValueAsString(request);

    if(!silentRequestsMode) {
      log.info("Post request({}): {}", request.getClass(), requestJson.length() > 2000 ? "{too large value}" : requestJson);
    }

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("Cookie", sessionCookies.stream().collect(Collectors.joining(";")));

    HttpEntity<String> requestHttp = new HttpEntity<>(requestJson, headers);

    try {
      String answer = getResult(restTemplate.exchange(endpoint + "/api/" + url, HttpMethod.POST, requestHttp, String.class));

      if (!silentRequestsMode) {
        log.info("Post response({}): {}", responseType, answer != null && answer.length() > 2000 ? "{too large value}" : answer);
      }

      // some controllers can response void, so we don't need mapping
      if (answer == null || responseType == null) {
        return null;
      }
      // some controllers send direct response with no structure, so it cannot be mapped by jackson
      if (responseType.equals(String.class)) {
        return (ResType) answer;
      }

      return mapper.readValue(answer, responseType);
    } catch (HttpClientErrorException e) {
      // handle existed file
      if (e.getStatusCode() == HttpStatus.CONFLICT) {
        throw e;
      } else {
        Assert.fail("Failed sending get request: " + e.getMessage());
      }
    } catch (Throwable throwable) {
      Assert.fail("Failed sending get request: " + throwable.getMessage());
    }

    return null;
  }

  protected <ResType> ResType sendGet(String url, MultiValueMap<String, String> params, Class<ResType> responseType) throws IOException {
    if(!silentRequestsMode) {
      log.info("GET request({}): {}", url, params);
    }

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("Cookie", sessionCookies.stream().collect(Collectors.joining(";")));

    HttpEntity<Object> requestHttp = new HttpEntity<>(headers);

    String answer;

    try {
      if(params != null) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(endpoint + url).queryParams(params);
        answer = getResult(restTemplate.exchange(builder.buildAndExpand(params).toUri(), HttpMethod.GET, requestHttp, String.class));
      } else {
        answer = getResult(restTemplate.exchange(endpoint + url, HttpMethod.GET, requestHttp, String.class));
      }

      if(!silentRequestsMode) {
        log.info("GET response({}): {}", responseType, answer);
      }

      return mapper.readValue(answer, responseType);

    } catch (Throwable throwable) {
      Assert.fail("Failed sending get request: " + throwable.getMessage());
    }

    return null;
  }

  protected <ResType> ResType sendGet(String url, Class<ResType> responseType) throws IOException {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("Cookie", sessionCookies.stream().collect(Collectors.joining(";")));

    HttpEntity<Object> requestHttp = new HttpEntity<>(headers);

    try {
      String answer = getResult(restTemplate.exchange(endpoint + url, HttpMethod.GET, requestHttp, String.class));
      return mapper.readValue(answer, responseType);
    } catch (Throwable throwable) {
      Assert.fail("Failed sending get request: " + throwable.getMessage());
    }

    return null;
  }

  // =================================================================================================================

  @Before
  public void setupWebContext() throws Exception {
    // make logged in user
    if (sessionCookies == null) {
      loginUser();
    }
  }

  // get rest template with SSL ignoring
  private static RestTemplate getRestTemplate() {
    try {
      TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;

      SSLContext sslContext = org.apache.http.ssl.SSLContexts.custom()
          .loadTrustMaterial(null, acceptingTrustStrategy)
          .build();

      SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext);

      CloseableHttpClient httpClient = HttpClients.custom()
          .setSSLSocketFactory(csf)
          .build();

      HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();

      requestFactory.setHttpClient(httpClient);

      return new RestTemplate(requestFactory);
    } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
      e.printStackTrace();
    }
    return null;
  }

  private void loginUser() throws Exception {
    String userLogin = "q";
    String userPassword = "q";

    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("login", userLogin);
    map.add("password", userPassword);
    map.add("anon", "");

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

    HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(map, headers);

    ResponseEntity<String> result = restTemplate.exchange(endpoint + "/api/auth/login", HttpMethod.POST, request, String.class);

    userId = mapper.readTree(result.getBody()).get("id").longValue();
    sessionCookies = result.getHeaders().get("Set-Cookie");

    checkLogin();
  }

  private void checkLogin() {
    // check login
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
    headers.set("Cookie", sessionCookies.stream().collect(Collectors.joining(";")));
    HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(map, headers);
    ResponseEntity<String> result = restTemplate.exchange(endpoint + "/api/user/state/" + userId, HttpMethod.GET, request, String.class);
    collector.checkThat(result.getStatusCode(), equalTo(HttpStatus.OK));

    // check unauthorized request
    HttpClientErrorException error = null;
    try {
      RestTemplate restTemplateUnauthorized = getRestTemplate();
      if (restTemplateUnauthorized != null) {
        restTemplateUnauthorized.getForEntity(endpoint + "/api/user/state/" + userId, String.class);
      }
    } catch (HttpClientErrorException e) {
      error = e;
    }
    collector.checkThat(error, notNullValue());
    if (error != null) {
      collector.checkThat(error.getStatusCode(), equalTo(HttpStatus.UNAUTHORIZED));
    }
  }

  protected void waitForTaskExecutionRestfully(String taskId, Long requestPeriod) throws InterruptedException, IOException {

    log.info("Waiting for execution of task with ID: {}", taskId);
    boolean finished = false;
    int step = 0;

    while (!finished) {
      await(requestPeriod);

      GetStateRequest getStateRequest = new GetStateRequest();
      getStateRequest.setTaskId(taskId);
      TaskInfo taskInfo = sendPost("flow/get_state", getStateRequest, TaskInfo.class);

      finished = taskInfo.isFinished() || taskInfo.isError() || taskInfo.isRemoved();

      // check if server fallen with 500 error
      if (step++ == 10) {
        sendPost("flow/get_active_tasks", "", FlowExecutionTasksResponse[].class);
        step = 0;
      }
    }

    log.info("Task with id {} has been finished.", taskId);
  }

  protected void await(Long time) throws InterruptedException {
    Thread.sleep(time);
  }

  // unpacking responses
  private String getResult(ResponseEntity request) {
    Assert.assertEquals(request.getStatusCode(), HttpStatus.OK);
    return request.getBody() == null ? null : request.getBody().toString();
  }

}