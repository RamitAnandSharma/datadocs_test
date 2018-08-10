package com.dataparse.server.controllers;

import com.dataparse.server.controllers.exception.*;
import com.dataparse.server.service.tasks.*;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import springfox.documentation.annotations.ApiIgnore;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApiIgnore
@Slf4j
public abstract class ApiController {

    protected static void doFileResponse(String contentType, String fileName, InputStream is, HttpServletResponse response) throws IOException {
        response.setContentType(contentType);
        response.setHeader("Content-disposition", "attachment; filename=" + fileName);
        IOUtils.copy(is, response.getOutputStream());
        response.flushBuffer();
    }

    public static final String API_ARG_NAME = "Datadocs-API-Arg";

    public static String getMessage(Throwable th) {
        if(th == null) {
            return "";
        } else {
            return th.getMessage();
        }
    }

    public static String getRootCauseMessage(Throwable th) {
        Throwable root = ExceptionUtils.getRootCause(th);
        root = root == null ? th:root;
        return getMessage(root);
    }

    @ExceptionHandler(ValidationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public List<ObjectError> handleException(ValidationException e) {
        return e.getErrors();
    }

    @ExceptionHandler(ExecutionException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public List<? extends ExecutionException.Error> handleException(ExecutionException e){
        return e.getErrors();
    }


    @ExceptionHandler(ForbiddenException.class)
    @ResponseStatus(value = HttpStatus.FORBIDDEN)
    public Map handleForbiddenException(ForbiddenException e) {
        Map<String, String> m = new HashMap<>();
        m.put("message", getRootCauseMessage(e));
        return m;
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    @ResponseStatus(value = HttpStatus.NOT_FOUND)
    public Map handleResourceNotFoundException(ResourceNotFoundException e) {
        Map<String, String> m = new HashMap<>();
        m.put("message", getRootCauseMessage(e));
        m.put("resourceId", String.valueOf(e.getResourceId()));
        m.put("deleted", String.valueOf(e.getDeleted()));
        m.put("resourceName", String.valueOf(e.getResourceName()));
        return m;
    }

    @ExceptionHandler(ResourceAlreadyExists.class)
    @ResponseStatus(value = HttpStatus.CONFLICT)
    public Map handleForbiddenException(ResourceAlreadyExists e) {
        Map<String, String> m = new HashMap<>();
        m.put("message", getRootCauseMessage(e));
        m.put("resourceId", e.getResourceId().toString());
        m.put("resourceName", e.getResourceName());
        return m;
    }

    @ExceptionHandler(RuntimeException.class)
    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    public Map handleRuntimeException(RuntimeException e) {
        log.error("Internal Server Error", e);
        Throwable rootException = ExceptionUtils.getRootCause(e);

        String message;
        if(rootException instanceof InvalidFormatException){
            InvalidFormatException ife = (InvalidFormatException) ExceptionUtils.getRootCause(e);
            message = "Can not cast \"" + ife.getValue().toString() + "\" to " + ife.getTargetType().getSimpleName();
        } else {
            message = getRootCauseMessage(e);
        }
        Map<String, String> m = new HashMap<>();
        m.put("message", message);
        m.put("stacktrace", ExceptionUtils.getStackTrace(e));
        return m;
    }

}
