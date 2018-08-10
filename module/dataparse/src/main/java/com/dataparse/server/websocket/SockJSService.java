package com.dataparse.server.websocket;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.auth.InstanceID;
import com.dataparse.server.service.notification.Event;
import org.apache.commons.lang3.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SockJSService implements IDatadocNotificationService {
    private Set<Long> connectedUsers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Set<String> connectedGuests = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Autowired
    private SimpMessagingTemplate simpMessagingTemplate;

    public void addUser(Auth auth) {
        if (auth.getUserId() != null) {
            connectedUsers.add(auth.getUserId());
        } else if (auth.getSessionId() != null) {
            connectedGuests.add(auth.getSessionId());
        }
    }

    public void removeUser(Auth auth) {
        if (auth.getUserId() != null) {
            connectedUsers.remove(auth.getUserId());
        } else if (auth.getSessionId() != null) {
            connectedGuests.remove(auth.getSessionId());
        }
    }

    public void send(Auth auth, String topic, Event event, boolean broadcast) {
        String userKey = auth.getUserId() == null ? auth.getSessionId() : String.valueOf(auth.getUserId());
        if (userKey != null) {
            event.setUser(auth.getUserId());
            event.setSessionId(auth.getSessionId());
            if (StringUtils.isBlank(event.getInstanceId())) {
                event.setInstanceId(InstanceID.get());
            }

            if (broadcast) {
                simpMessagingTemplate.convertAndSend("/all" + topic, event);
            } else {
                simpMessagingTemplate.convertAndSendToUser(userKey, topic, event);
            }
        }
    }

    // TODO: Redo this "Conceptual trash" (2)
    public void unsubscribeUser(Auth auth) { }

    public void send(Auth auth, String topic, Event event) {
        this.send(auth, topic, event, false);
    }
}
