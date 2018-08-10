package com.dataparse.server.util;

import com.dataparse.server.controllers.exception.ResourceNotFoundException;
import com.dataparse.server.service.files.AbstractFile;

public class ApiUtils {
    public static void checkExisting(Long id, AbstractFile file) throws ResourceNotFoundException {
        if(file == null) {
            throw new ResourceNotFoundException(id);
        } else if(file.isDeleted()) {
            throw new ResourceNotFoundException(file.getId(), file.getName(), file.isDeleted());
        }

    }
}
