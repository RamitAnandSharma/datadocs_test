package com.dataparse.server.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@AllArgsConstructor
@RequiredArgsConstructor
@Data
class InitialUser {

    private final String fullName;
    private final String email;
    private final String password;
    private boolean admin = true;

}