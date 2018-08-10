package com.dataparse.server.service.files;

import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.user.User;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.io.Serializable;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@AssociationOverrides({
        @AssociationOverride(name = "primaryKey.user",
                joinColumns = @JoinColumn(name = "user_id")),
        @AssociationOverride(name = "primaryKey.datadoc",
                joinColumns = @JoinColumn(name = "datadoc_id")) })
public class UserFileShare implements Serializable {
    @EmbeddedId
    @JsonIgnore
    private UserFileShareId primaryKey;
    private ShareType shareType;
    @ManyToOne(fetch = FetchType.EAGER)
    private User shareFrom;
    @ManyToOne(fetch = FetchType.EAGER)
    private User owner;

    public UserFileShare(User user, Datadoc datadoc, ShareType shareType, User shareFrom, User owner) {
        this(new UserFileShareId(user, datadoc), shareType, shareFrom, owner);
    }

    @Transient
    @JsonIgnore
    public User getUser() {
        return primaryKey.getUser();
    }

    @Transient
    public String getEmail() {
        return primaryKey.getUser().getEmail();
    }

    @Transient
    public String getAvatarPath() {
        return primaryKey.getUser().getAvatarPath();
    }

    @Transient
    public Long getUserId() {
        return primaryKey.getUser().getId();
    }

    @Transient
    public String getName() {
        return primaryKey.getUser().getFullName();
    }

    @Transient
    @JsonIgnore
    public Datadoc getDatadoc() {
        return primaryKey.getDatadoc();
    }


}
