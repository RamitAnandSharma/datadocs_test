package com.dataparse.server.websocket.interceptors;

import com.dataparse.server.auth.Auth;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.ExecutorChannelInterceptor;

import java.util.Map;

public class CommonSockJSInterceptor implements ExecutorChannelInterceptor {
    @Override
    public Message<?> beforeHandle(Message<?> message, MessageChannel channel, MessageHandler handler) {
        return message;
    }

    @Override
    public void afterMessageHandled(Message<?> message, MessageChannel channel, MessageHandler handler, Exception ex) {

    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        return message;
    }

    @Override
    public void postSend(Message<?> message, MessageChannel channel, boolean sent) {

    }

    @Override
    public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent, Exception ex) {

    }

    @Override
    public boolean preReceive(MessageChannel channel) {
        return true;
    }

    @Override
    public Message<?> postReceive(Message<?> message, MessageChannel channel) {
        return message;
    }

    @Override
    public void afterReceiveCompletion(Message<?> message, MessageChannel channel, Exception ex) {

    }

    public Auth getAuth(Map<String, Object> attributes) {
        Auth auth = new Auth();
        if (attributes != null) {
            auth.setUserId((Long) attributes.get(Auth.CURRENT_USER));
            auth.setSessionId((String) attributes.get(Auth.CURRENT_SESSION_ID));
        }
        return auth;
    }
}
