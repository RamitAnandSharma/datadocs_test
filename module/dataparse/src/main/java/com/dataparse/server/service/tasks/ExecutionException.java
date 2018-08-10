package com.dataparse.server.service.tasks;

import com.fasterxml.jackson.annotation.*;
import lombok.*;

import java.util.*;

@Data
public class ExecutionException extends RuntimeException {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    public static class Error {

        public Error(final String code, final String message) {
            this.code = code;
            this.message = message;
        }

        String code;
        String message;
        String description;
    }

    private List<? extends Error> errors;

    private ExecutionException(List<? extends Error> errors) {
        super(errors.get(0).getMessage() + (errors.size() > 1 ? " and " + (errors.size() - 1) + " more..." : ""));
        this.errors = errors;
    }

    public Error getFirstError(){
        return this.errors.get(0);
    }

    public static ExecutionException of(String errorCode, String errorMessage){
        return of(errorCode, errorMessage, null);
    }

    public static ExecutionException of(String errorCode, String errorMessage, String errorDescription){
        return new ExecutionException(Collections.singletonList(new Error(errorCode, errorMessage, errorDescription)));
    }

    public static ExecutionException of(List<? extends Error> errors){
        return new ExecutionException(errors);
    }



}
