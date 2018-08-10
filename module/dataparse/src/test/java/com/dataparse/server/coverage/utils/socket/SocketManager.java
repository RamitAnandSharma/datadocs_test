package com.dataparse.server.coverage.utils.socket;

import com.dataparse.server.service.notification.Event;
import com.dataparse.server.util.SystemUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSession.Subscription;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;


@Slf4j
public class SocketManager {

  private String protocol = SystemUtils.getProperty("PROTOCOL", "http").equals("http") ? "ws" : "wss";
  private String endpoint = SystemUtils.getProperty("ENDPOINT", "localhost:9100");

  private ObjectMapper mapper = new ObjectMapper();
  private List<String> sessionCookies;
  private Long userId;

  private StompSession stompSession;
  private Boolean processingRequest = false;
  private Boolean silentAwaitMessagesMode = true;

  public SocketManager(List<String> sessionCookies, Long userId) {

    this.sessionCookies = sessionCookies;
    this.userId = userId;

    connect();
  }

  // =================================================================================================================

  public void connect() {
    log.info("Connecting to sever socket...");

    List<Transport> transports = new ArrayList<>(1);
    transports.add(new WebSocketTransport(new StandardWebSocketClient()));

    WebSocketStompClient stompClient = new WebSocketStompClient(new SockJsClient(transports));

    int messageSizeLimit = stompClient.getInboundMessageSizeLimit() * 10;
    stompClient.setInboundMessageSizeLimit(messageSizeLimit);
    stompClient.setMessageConverter(new MappingJackson2MessageConverter());

    HttpHeaders headers = new HttpHeaders();
    headers.set("Cookie", sessionCookies.stream().collect(Collectors.joining(";")));

    int maxRetries = 3;
    int retries = 0;

    while(retries < maxRetries) {
      try {
        stompSession = stompClient.connect(
            protocol + "://" + endpoint + "/websocket",
            new WebSocketHttpHeaders(headers),
            new StompSessionHandlerAdapter() {
            }).get();

        log.info("Connected to web server socket...");
        return;
      } catch (Exception e) {
        log.error("Failed creating stompsession...");
        retries ++;
      }
    }
  }

  // =================================================================================================================

  public <T extends Event> void sendEventWithoutSubscription(T event) throws IOException {
    log.info("Sending {} event.", event.getClass());

    byte[] rawMessage = preprocessEvent(event);
    stompSession.send("/websocket/vis/event", rawMessage);
  }

  public <T extends Event> void sendEventAndAwait(T event, Long tabId, UUID stateId) throws InterruptedException, IOException {

    ensureSocketConection();

    processingRequest = true;
    byte[] rawMessage = preprocessEvent(event);

    Subscription subscription = getVISEventSubscription(tabId, stateId);
    sendRawEventData("/websocket/vis/event", rawMessage);
    awaitResponse();
    removeSubscription(subscription);
  }

  // =================================================================================================================

  // hack for jackson mapper directory resolving
  private <T extends Event> byte[] preprocessEvent(T event) throws IOException {
    String msg = mapper.writeValueAsString(event);

    for (String category : Arrays.asList("request", "shows", "filter", "aggs", "ingest")) {
      String path = "com.dataparse.server.service.visualization.bookmark_state.event." + category;
      ImmutableSet<ClassPath.ClassInfo> topLevelClasses = ClassPath.from(Thread.currentThread().getContextClassLoader()).getTopLevelClasses(path);
      List<String> res = topLevelClasses.stream().map(ClassPath.ClassInfo::getSimpleName).collect(Collectors.toList());

      String regexReplace = String.join("|", res);
      msg = msg.replaceAll("@type\":\"\\.(" + regexReplace + ")\"", "@type\":\"." + category + ".$1\"");
    }

    return msg.getBytes();
  }

  private void ensureSocketConection() {
    if (stompSession == null || !stompSession.isConnected()) {
      connect();
    }
  }

  private void awaitResponse() throws InterruptedException {
    if (!silentAwaitMessagesMode) {
      log.info("Waiting for request processing...");
    }

    while (processingRequest) {
      Thread.sleep(100);
    }

    if (!silentAwaitMessagesMode) {
      log.info("Processing finished...");
    }
  }

  private void sendRawEventData(String endpoint, byte[] data) {
    try {
      // send message into topic
      stompSession.send(endpoint, data);
    } catch (IllegalStateException e) {
      // could be closed at the end of tests
      e.printStackTrace();
    }
  }

  private Subscription getVISEventSubscription(Long tabId, UUID stateId) {
    try {
      return stompSession.subscribe("/user/" + userId + "/vis/event-response/" + tabId + "/" + stateId,
          new StompSessionHandlerLocal());
    } catch (IllegalStateException e) {
      e.printStackTrace();
      return null;
    }
  }

  private void removeSubscription(Subscription subscription) {
    if (subscription != null) {
      subscription.unsubscribe();
    }
  }

  // =================================================================================================================

  private class StompSessionHandlerLocal extends StompSessionHandlerAdapter {
    @Override
    public void handleException(StompSession stompSession, StompCommand stompCommand, StompHeaders stompHeaders, byte[] bytes, Throwable throwable) {
      processingRequest = false;
    }

    @Override
    public void handleTransportError(StompSession stompSession, Throwable throwable) {
      processingRequest = false;
    }

    @Override
    public void handleFrame(StompHeaders headers, Object payload) {
      processingRequest = false;
    }

    @Override
    public Type getPayloadType(StompHeaders stompHeaders) {
      return JsonNode.class;
    }
  }

}
