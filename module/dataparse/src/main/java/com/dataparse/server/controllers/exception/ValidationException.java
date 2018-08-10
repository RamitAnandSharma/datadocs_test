package com.dataparse.server.controllers.exception;

import lombok.Data;
import org.springframework.validation.ObjectError;

import java.util.List;

/**
 * Deprecated. Please use FormValidationException.
 */
@Data
@Deprecated
public class ValidationException extends RuntimeException
{
    private List<ObjectError> errors;

    public ValidationException(List<ObjectError> errors) {
        this.errors = errors;
    }
}
