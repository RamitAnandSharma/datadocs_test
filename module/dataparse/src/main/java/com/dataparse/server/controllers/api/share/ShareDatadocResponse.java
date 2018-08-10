package com.dataparse.server.controllers.api.share;


import com.dataparse.server.service.files.UserFileShare;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ShareDatadocResponse {
    private List<String> failedShared = new ArrayList<>();
    private List<UserFileShare> successfullyShared = new ArrayList<>();


    public void addSuccess(UserFileShare name) {
        this.successfullyShared.add(name);
    }

    public void addFailed(String reason) {
        this.failedShared.add(reason);
    }

}
