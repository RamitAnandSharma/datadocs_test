package com.dataparse.server.service.mail;

import com.dataparse.server.config.AppConfig;
import lombok.AllArgsConstructor;
import org.thymeleaf.context.Context;

@AllArgsConstructor
public class ExportReadyEmail extends AbstractEmail {
    private String sourceName;
    private String exportId;

    private String getDownloadLink() {
        return AppConfig.BASE_URL + "/api/export/download?requestId=" + exportId;
    }

    @Override
    public Context getContext() {
        Context context = new Context();

        context.setVariable("sourceName", sourceName);
        context.setVariable("downloadLink", getDownloadLink());
        context.setVariable("logoUrl", LOGO_URL);

        return context;
    }

    @Override
    public String templateFile() {
        return "export-ready";
    }

    @Override
    public String getSubject() {
        return "Export for " + sourceName + " finished";
    }
}
