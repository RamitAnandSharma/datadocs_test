package com.dataparse.server.controllers.api.share;

import com.dataparse.server.service.files.ShareType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class ShareDatadocRequest {
    private Long datadocId;
    private List<String> email;
    private ShareType shareType;
    private String noteText;
    private Boolean shareAttachedSources;
}
