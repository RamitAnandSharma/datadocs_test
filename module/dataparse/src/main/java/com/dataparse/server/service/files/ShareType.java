package com.dataparse.server.service.files;

public enum  ShareType {
    ADMIN(1),
    VIEW(0);

    private Integer proprity;

    ShareType(Integer priority) {
        this.proprity = priority;
    }

    public boolean isAllowed(ShareType shareType) {
        return shareType.proprity >= this.proprity;
    }

    public boolean isAdmin() {
        return ADMIN.equals(this);
    }
}
