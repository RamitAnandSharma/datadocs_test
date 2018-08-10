package com.dataparse.server.service.mail;

import com.dataparse.server.config.AppConfig;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.upload.Upload;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.thymeleaf.context.Context;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DisconnectedSourceEmail extends AbstractEmail {
    private Upload source;
    private Datadoc datadoc;
    private Boolean refresh;

    private String getReference() {
        if(refresh) {
            String urlStatePart = "/visualize/" + datadoc.getId();
            return AppConfig.SYSTEM_URL + urlStatePart;
        }
        return null;
    }

    public String getUserName() {
        if(this.datadoc != null) {
            return this.datadoc.getUser().getName();
        } else if(this.source != null) {
            return this.source.getUser().getName();
        }
        return null;
    }

    @Override
    public Context getContext() {
        Context context = new Context();
        context.setVariable("refresh", refresh);
        context.setVariable("datadocUrl", getReference());
        context.setVariable("sourceName", source.getName());
        context.setVariable("logoUrl", LOGO_URL);
        context.setVariable("userName", getUserName());
        return context;
    }

    @Override
    public String templateFile() {
        return "refresh-failed";
    }

    @Override
    public String getSubject() {
        if(refresh) {
            return "Your datadoc " + datadoc.getName() + " failed to refresh.";
        } else {
            return "Your source " + source.getName() + " has been disconnected.";
        }
    }
}
