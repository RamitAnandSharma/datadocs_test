package com.dataparse.server.service.security;

import com.dataparse.server.service.files.ShareType;
import com.google.common.collect.Sets;

import java.util.Set;

public enum DatadocActionAccessibility {

    CREATE_BOOKMARK(ShareType.ADMIN),
    UPDATE_BOOKMARK(ShareType.ADMIN),
    DELETE_BOOKMARK(ShareType.ADMIN),
    MOVE_BOOKMARK(ShareType.ADMIN),
    UNDO_REDO_BOOKMARK(ShareType.ADMIN),

    GET_BOOKMARK(ShareType.values()),
    GET_BOOKMARK_MAPPING(ShareType.values()),

    GET_FILE(ShareType.values()),

    UPDATE_SETTINGS(ShareType.ADMIN),
    FILTER_DATA(ShareType.values());

    private Set<ShareType> shareTypes;

    DatadocActionAccessibility(ShareType ... shareType) {
        this.shareTypes = Sets.newHashSet(shareType);
    }

    public boolean isAccessible(ShareType userShareType) {
        return this.shareTypes.contains(userShareType);
    }

}
