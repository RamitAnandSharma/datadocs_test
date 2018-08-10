package com.dataparse.server.websocket.interceptors;

import com.dataparse.server.auth.Auth;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;

public class SubscriptionInterceptor extends CommonSockJSInterceptor {
    @Override
    public Message<?> beforeHandle(Message<?> message, MessageChannel channel, MessageHandler handler) {
        SimpMessageHeaderAccessor accessor = SimpMessageHeaderAccessor.wrap(message);
        Auth auth = getAuth(accessor.getSessionAttributes());
        String userId = auth.getUserId() != null ? auth.getUserId().toString() : auth.getSessionId();

        // TODO: simplify condition?
        if (userId != null && accessor.getMessageType() == SimpMessageType.SUBSCRIBE
                && !accessor.getDestination().startsWith("/user/" + userId + "/")
                && !accessor.getDestination().startsWith("/all/")) {
            return null;
        }
        return message;
    }
}
