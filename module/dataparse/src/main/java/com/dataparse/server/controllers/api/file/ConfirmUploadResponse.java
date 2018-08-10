package com.dataparse.server.controllers.api.file;

import com.dataparse.server.service.files.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConfirmUploadResponse {

    private File file;

}
