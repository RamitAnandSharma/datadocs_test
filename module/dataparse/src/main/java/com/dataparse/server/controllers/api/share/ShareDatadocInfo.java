package com.dataparse.server.controllers.api.share;

import com.dataparse.server.config.AppConfig;
import com.dataparse.server.service.files.ShareType;
import com.dataparse.server.service.files.UserFileShare;
import com.dataparse.server.service.user.User;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ShareDatadocInfo {
    private List<UserFileShare> sharedWith;
    private User datadocOwner;
    private ShareType shareType;
    private Boolean publicShared;
    private UUID sharePrefix;
    private UUID sharedStateId;

    public String getShareUrlTemplate() {
        return String.format("%s/shared/%s", AppConfig.SYSTEM_URL, sharePrefix);
    }
}
