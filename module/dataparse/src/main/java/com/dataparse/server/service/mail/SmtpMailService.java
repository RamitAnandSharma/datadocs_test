package com.dataparse.server.service.mail;

import com.dataparse.server.util.FunctionUtils;
import com.dataparse.server.util.SystemUtils;
import com.sendgrid.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;

import java.io.IOException;

@Service
@Slf4j
public class SmtpMailService implements MailService {

    @Autowired
    private TemplateEngine templateEngine;

    @Autowired
    private SendGrid sendGrid;

    private static final String FROM = "no-reply@test1.datadocs.com";
    private static final boolean DISABLE_EMAILS = SystemUtils.getProperty("DISABLE_EMAILS", false);

    @Override
    public void send(String address, String subject, String body, String replyTo, String whoSend, FunctionUtils.SimpleFunction callback) {
        try {
            if (DISABLE_EMAILS) {
                return;
            }
            Email from = new Email(FROM, String.format("%s (via Datadocs)", whoSend));
            Email to = new Email(address);
            Content content = new Content("text/html", body);
            Mail mail = new Mail(from, subject, to, content);
            mail.setReplyTo(new Email(replyTo, whoSend));
            Request request = new Request();
            request.setMethod(Method.POST);
            request.setBody(mail.build());
            request.setEndpoint("mail/send");

            sendGrid.api(request);
            callback.invoke();
        } catch (IOException e) {
            throw new RuntimeException("Can not send email. ", e);
        }
    }

    @Override
    public void send(String address, AbstractEmail abstractEmail, FunctionUtils.SimpleFunction callback) {
        if(DISABLE_EMAILS) {
            return;
        }
        String body = templateEngine.process(abstractEmail.templateFile(), abstractEmail.getContext());
        send(address, abstractEmail.getSubject(), body, abstractEmail.getReplyTo(), abstractEmail.getWhoSend(), callback);
    }

    @Override
    public void send(String address, String title, String body, String replyTo, String whoSend) {
        this.send(address, title, body, replyTo, whoSend, FunctionUtils.doNothing);
    }

    @Override
    public void send(String address, AbstractEmail abstractEmail) {
        this.send(address, abstractEmail, FunctionUtils.doNothing);
    }
}
