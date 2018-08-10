package com.dataparse.server.controllers.api.file;

import com.dataparse.server.service.upload.FormatSettings;
import lombok.Data;

@Data
public class UpdateSettingsRequest {

    String path;
    Long tabId;
    FormatSettings settings;

}
