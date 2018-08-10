package com.dataparse.server.controllers.api.file;

import com.dataparse.server.service.upload.Upload;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ExistsFileInfo {
    private Long id;
    private Long parentId;
    private String name = "Untitled datadoc";
    private String checksum;

    public ExistsFileInfo(String checksum, Upload upload) {
        this.checksum = checksum;
        if(upload != null) {
            this.id = upload.getId();
            this.parentId = upload.getParentId();
        }
    }
    public ExistsFileInfo(String checksum) {
        this(null, null, null,checksum);
    }
}
