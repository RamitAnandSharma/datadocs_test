package com.dataparse.server.auth;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.MDC;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Auth {

    private Long userId;
    private String sessionId;

    public static final String CURRENT_USER = "CURRENT_USER";
    public static final String CURRENT_SESSION_ID = "CURRENT_SESSION_ID";

    public static Auth get() {
        Long userId = null;
        String sessionId = null;
        if (RequestContextHolder.getRequestAttributes() != null) {
            userId = (Long) RequestContextHolder.getRequestAttributes().getAttribute(CURRENT_USER, RequestAttributes.SCOPE_SESSION);
            sessionId = (String) RequestContextHolder.getRequestAttributes().getAttribute(CURRENT_SESSION_ID, RequestAttributes.SCOPE_SESSION);
        } else {
            String userIdString = MDC.get(CURRENT_USER);
            if(userIdString != null){
                userId = Long.parseLong(userIdString);
            }
            sessionId = MDC.get(CURRENT_SESSION_ID);
        }
        return new Auth(userId, sessionId);
    }

    public static void set(Auth auth) {
        setCurrentUser(auth.getUserId());
        setCurrentSessionId(auth.getSessionId());
    }

    public static boolean isAuthenticated() {
        return get().getUserId() != null;
    }

    public static void setCurrentUser(Long userId) {
        if(RequestContextHolder.getRequestAttributes() != null) {
            RequestContextHolder.getRequestAttributes().setAttribute(CURRENT_USER, userId, RequestAttributes.SCOPE_SESSION);
        }
        if(userId != null) {
            MDC.put(CURRENT_USER, userId.toString());
        }
    }

    public static void setCurrentSessionId(String sessionId) {
        if(RequestContextHolder.getRequestAttributes() != null) {
            RequestContextHolder.getRequestAttributes().setAttribute(CURRENT_SESSION_ID, sessionId, RequestAttributes.SCOPE_SESSION);
        }
        if(sessionId != null){
            MDC.put(CURRENT_SESSION_ID, sessionId);
        }
    }

    public static void removeCurrentUser() {
        if(RequestContextHolder.getRequestAttributes() != null) {
            RequestContextHolder.getRequestAttributes().removeAttribute(CURRENT_USER, RequestAttributes.SCOPE_SESSION);
        }
    }
}
