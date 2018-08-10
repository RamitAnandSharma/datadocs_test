package com.dataparse.server.service.mail;

import com.dataparse.server.config.AppConfig;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.files.ShareType;
import com.dataparse.server.service.user.User;
import lombok.AllArgsConstructor;
import org.thymeleaf.context.Context;

@AllArgsConstructor
public class DatadocShareEmail extends AbstractEmail {
    private Datadoc datadoc;
    private User shareFrom;
    private String noteText;
    private ShareType shareType;
    private Boolean registeredUser;

    private String getSharedLink() {
        String urlStatePart = "/visualize/" + datadoc.getId();
        String baseUrl = AppConfig.SYSTEM_URL + urlStatePart;
        if(registeredUser) {
            return baseUrl;
        } else {
            return String.format("%s/register?state=main.visualize&param=id:%s", AppConfig.SYSTEM_URL, datadoc.getId());
        }
    }

    @Override
    public Context getContext() {
        Context context = new Context();
        context.setVariable("userRole", ShareType.ADMIN.equals(shareType) ? "be an admin on" : "view");
        context.setVariable("sharedLink", getSharedLink());
        context.setVariable("datadocName", datadoc.getName());
        context.setVariable("shareFrom", shareFrom.getFullName());
        context.setVariable("noteText", noteText);
        context.setVariable("logoUrl", LOGO_URL);
        return context;
    }

    @Override
    public String getSubject() {
        return String.format("%s shared %s with you", shareFrom.getFullName(), datadoc.getName());
    }

    @Override
    public String getReplyTo() {
        return shareFrom.getEmail();
    }

    @Override
    public String getWhoSend() {
        return shareFrom.getFullName();
    }

    @Override
    public String templateFile() {
        return "share-mail";
    }
}
