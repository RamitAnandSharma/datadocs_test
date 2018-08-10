package com.dataparse.server.controllers.exception;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@NoArgsConstructor
@AllArgsConstructor
@Data
@ResponseStatus(HttpStatus.NOT_FOUND)
public class ResourceNotFoundException extends RuntimeException {
    private Long resourceId;
    private String resourceName;
    private Boolean deleted = false;

    public ResourceNotFoundException(Long resourceId) {
        this.resourceId = resourceId;
    }

}
