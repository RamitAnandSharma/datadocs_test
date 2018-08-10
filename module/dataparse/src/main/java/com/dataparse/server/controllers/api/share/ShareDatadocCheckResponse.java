package com.dataparse.server.controllers.api.share;

import com.dataparse.server.service.files.ShareType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShareDatadocCheckResponse {
    private ShareType shareType;
    private Boolean datadocShared;
    private Boolean isOwner;

    public Boolean isAccessible() {
        return datadocShared || isOwner;
    }


    public static final ShareDatadocCheckResponse NOT_SHARED = new ShareDatadocCheckResponse(null, false, false);
}
