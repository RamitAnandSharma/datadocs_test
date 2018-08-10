package com.dataparse.server.service.security;


import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.exception.ForbiddenException;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.files.UserFileShare;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.upload.UploadRepository;
import com.dataparse.server.service.user.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DatadocSecurityService {

    @Autowired
    private UploadRepository uploadRepository;

    public void checkDatadocAccess(Long datadocId, DatadocActionAccessibility actionAccessibility) throws ForbiddenException {
        Datadoc datadoc = uploadRepository.getDatadocById(datadocId);
        checkDatadocAccess(datadoc, actionAccessibility);
    }

    public void checkDatadocAccess(TableBookmark tableBookmark, DatadocActionAccessibility actionAccessibility) throws ForbiddenException {
        checkDatadocAccess(tableBookmark.getDatadoc(), actionAccessibility);
    }

    public void checkDatadocAccess(Datadoc datadoc, DatadocActionAccessibility actionAccessibility) throws ForbiddenException {
        Long userId = Auth.get().getUserId();
        boolean accessible = isAccessible(datadoc, userId, actionAccessibility);
        if(!accessible) {
            throw new ForbiddenException(ErrorMessages.getGeneralErrorMessage(datadoc.getUser()));
        }
    }

    private boolean isAccessible(Datadoc datadoc, Long userId, DatadocActionAccessibility actionAccessibility) throws ForbiddenException {
        if(datadoc.getPublicShared()) {
            return true;
        }
        if(datadoc.getUser().getId().equals(userId)) {
//            it's owner, he can do everything
            return true;
        }

        UserFileShare userShare = datadoc.getSharedWithById().get(userId);

        if(userShare == null) {
            throw new ForbiddenException(ErrorMessages.getGeneralErrorMessage(datadoc.getUser()));
        } else {
            return actionAccessibility.isAccessible(userShare.getShareType());
        }
    }


    private static class ErrorMessages {
        private static final String GENERAL_ERROR_MESSAGE = "You do not have access to this datadoc. You could contact %s.";

        private static String getGeneralErrorMessage(User owner) {
            return String.format(GENERAL_ERROR_MESSAGE, owner.getEmail());
        }
    }


}
