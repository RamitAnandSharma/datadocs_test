package com.dataparse.server.controllers.api.file;

import com.dataparse.server.service.entity.*;
import lombok.*;

@Data
public class CloseStickyNoteRequest {

    private String path;
    private StickyNoteType stickyNoteType;

}
