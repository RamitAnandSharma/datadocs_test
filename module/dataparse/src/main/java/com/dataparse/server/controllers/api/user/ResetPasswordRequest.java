package com.dataparse.server.controllers.api.user;

import lombok.Data;

@Data
public class ResetPasswordRequest {

    String token;
    String password;

}
