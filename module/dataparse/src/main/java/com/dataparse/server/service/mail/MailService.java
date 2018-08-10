package com.dataparse.server.service.mail;


import com.dataparse.server.util.FunctionUtils;

public interface MailService {

    void send(String address, String title, String body, String replyTo, String whoSend);
    void send(String address, AbstractEmail abstractEmail);
    void send(String address, String title, String body, String replyTo, String whoSend, FunctionUtils.SimpleFunction callback);
    void send(String address, AbstractEmail abstractEmail, FunctionUtils.SimpleFunction callback);


}
