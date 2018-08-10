package com.dataparse.server.controllers.api.table;

import com.dataparse.server.service.upload.RefreshSettings;
import lombok.Data;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Data
public class UpdateRefreshSettingsRequest {

    @Valid
    RefreshSettings settings;

}
