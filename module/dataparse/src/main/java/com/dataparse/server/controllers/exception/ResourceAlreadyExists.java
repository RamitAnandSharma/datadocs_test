package com.dataparse.server.controllers.exception;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@NoArgsConstructor
@AllArgsConstructor
@Data
@ResponseStatus(HttpStatus.CONFLICT)
public class ResourceAlreadyExists extends RuntimeException {
    private String resourceType;
    private String resourceName;
    private Long resourceId;

    @Override
    public String getMessage() {
        return String.format("%s %s already exists.", resourceType, resourceName);
    }
}
