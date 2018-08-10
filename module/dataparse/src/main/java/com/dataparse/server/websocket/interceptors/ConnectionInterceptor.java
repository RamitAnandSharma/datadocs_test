package com.dataparse.server.websocket.interceptors;

import com.dataparse.server.auth.*;
import com.dataparse.server.service.docs.DatadocNotificationService;
import com.dataparse.server.websocket.IDatadocNotificationService;
import com.dataparse.server.websocket.SockJSService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;

import java.util.Arrays;
import java.util.List;

public class ConnectionInterceptor extends CommonSockJSInterceptor {
    @Autowired
    private SockJSService sockJSService = new SockJSService();

    @Autowired
    private DatadocNotificationService datadocNotificationService = new DatadocNotificationService();

    @Override
    public Message<?> beforeHandle(final Message<?> message, final MessageChannel channel,
                                   final MessageHandler handler) {
        SimpMessageHeaderAccessor accessor = SimpMessageHeaderAccessor.wrap(message);
        Auth auth = getAuth(accessor.getSessionAttributes());
        switch (accessor.getMessageType()) {
            case MESSAGE:
                Auth.set(auth);
                break;
        }
        return super.beforeHandle(message, channel, handler);
    }

    @Override
    public void afterMessageHandled(Message<?> message, MessageChannel channel, MessageHandler handler, Exception ex) {
        SimpMessageHeaderAccessor accessor = SimpMessageHeaderAccessor.wrap(message);
        Auth auth = getAuth(accessor.getSessionAttributes());

        List<IDatadocNotificationService> connectionListeners = Arrays.asList(sockJSService, datadocNotificationService);

        switch (accessor.getMessageType()) {
            case CONNECT:
                connectionListeners.forEach(listener -> listener.addUser(auth));
                break;
            case DISCONNECT:
                connectionListeners.forEach(listener -> listener.removeUser(auth));
                break;
            case UNSUBSCRIBE:
                if (accessor.getDestination() != null
                        && accessor.getDestination().contains(DatadocNotificationService.getDatadocEventTopic())) {
                    datadocNotificationService.unsubscribeUser(auth);
                }
                break;
        }
    }
}
