package com.dataparse.server.websocket;

import com.dataparse.server.auth.Auth;

public interface IDatadocNotificationService {

    void addUser(Auth auth);

    void removeUser(Auth auth);

}