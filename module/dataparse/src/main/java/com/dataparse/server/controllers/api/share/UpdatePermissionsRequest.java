package com.dataparse.server.controllers.api.share;

import com.dataparse.server.service.files.ShareType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UpdatePermissionsRequest extends AbstractSingleShareRequest {
    private ShareType shareType;
}
