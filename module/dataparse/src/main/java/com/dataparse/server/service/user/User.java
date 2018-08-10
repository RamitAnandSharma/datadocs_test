package com.dataparse.server.service.user;

import com.dataparse.server.service.entity.*;
import com.fasterxml.jackson.annotation.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.Date;
import java.util.TimeZone;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
public class User extends BasicEntity {

    private String email;
    private String password;
    private String avatarPath;
    private String fullName;
    private TimeZone timezone;
    private Boolean admin = false;

    @JsonIgnore
    private String passwordResetToken;
    @JsonIgnore
    private Date passwordResetDate;
    private Boolean registered;

    public User(String email, String password) {
        this.email = email;
        this.password = password;
    }

    @Transient
    private String sessionId;

    @Transient
    public String getName() {
        return fullName;
    }

    @Transient
    private boolean manualEngineSelection;

}
