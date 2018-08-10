package com.dataparse.server.service.mail;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Primary
@Service
public class MailSendExecutor extends SmtpMailService {
    private ExecutorService mailSendExecutor;

    @PostConstruct
    public void init() {
        mailSendExecutor = Executors.newFixedThreadPool(2);
    }

    @Override
    public void send(String address, String subject, String body, String replyTo, String whoSend) {
        mailSendExecutor.execute(() -> {
            try {
                super.send(address, subject, body, replyTo, whoSend);
            } catch (Exception e) {
                log.warn("Can not send email to {}", address, e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void send(String address, AbstractEmail abstractEmail) {
        mailSendExecutor.execute(() -> {
            try {
                super.send(address, abstractEmail);
            } catch (Exception e) {
                log.warn("Can not send email to {}", address, e);
                throw new RuntimeException(e);
            }
        });
    }
}
