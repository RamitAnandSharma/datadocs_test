package com.dataparse.server.service.user;

import lombok.Data;

@Data
public class UserDTO {

    public UserDTO(User user) {
        userId = user.getId();
        fullName = user.getFullName();
        email = user.getEmail();
        avatarPath = user.getAvatarPath();
    }

    private Long userId;

    private String fullName;

    private String email;

    private String avatarPath;

}
