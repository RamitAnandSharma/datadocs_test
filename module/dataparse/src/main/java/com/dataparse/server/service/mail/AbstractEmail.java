package com.dataparse.server.service.mail;

import com.dataparse.server.config.AppConfig;
import org.thymeleaf.context.Context;

import static com.dataparse.server.config.AppConfig.BASE_URL;

public abstract class AbstractEmail {

    protected String LOGO_URL = BASE_URL + "/static/img/datadocs-logo.png";

    public abstract Context getContext();
    public abstract String templateFile();
    public abstract String getSubject();

    public String getReplyTo() {
        return "no-reply@" + AppConfig.URL_WITHOUT_PROTOCOL;
    }

    public String getWhoSend() {
        return "Datadocs";
    }
}
