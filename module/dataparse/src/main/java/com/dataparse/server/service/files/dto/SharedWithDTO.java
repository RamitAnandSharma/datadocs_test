package com.dataparse.server.service.files.dto;

import com.dataparse.server.service.files.UserFileShare;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
public class SharedWithDTO {
    private final static int DEFAULT_EMAILS_COUNT = 3;

    private List<String> sharedWith;
    private Integer allSharedCount;

    public SharedWithDTO(List<UserFileShare> sharedWith) {
        allSharedCount = sharedWith.size();
        int endIndex = sharedWith.size() > DEFAULT_EMAILS_COUNT ? DEFAULT_EMAILS_COUNT : sharedWith.size();
        this.sharedWith = sharedWith.subList(0, endIndex).stream().
                map(share -> share.getUser().getFullName()).
                collect(Collectors.toList());
    }

    public Integer getHideUsersCount() {
        return allSharedCount - sharedWith.size();
    }
}
