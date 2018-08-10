package com.dataparse.server.controllers;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.share.*;
import com.dataparse.server.controllers.exception.ForbiddenException;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.files.ShareType;
import com.dataparse.server.service.files.UserFileShare;
import com.dataparse.server.service.mail.DatadocShareEmail;
import com.dataparse.server.service.mail.MailService;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.share.ShareService;
import com.dataparse.server.service.upload.UploadRepository;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.visualization.bookmark_state.dto.BookmarkStateViewDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/share")
public class ShareController {

    @Autowired
    private ShareService shareService;

    @Autowired
    private UploadRepository uploadRepository;

    @Autowired
    private TableService tableService;

    @Autowired
    private MailService mailService;

    @PostMapping
    public ShareDatadocResponse shareDatadoc(@RequestBody ShareDatadocRequest request) {
        Datadoc datadoc = uploadRepository.getDatadocById(request.getDatadocId());
        ShareDatadocResponse response = shareService.shareDatadoc(datadoc, request);

        response.getSuccessfullyShared().forEach(userFileShare -> {
            try {
                String sendTo = userFileShare.getUser().getEmail();
                Boolean registered = userFileShare.getUser().getRegistered();

                DatadocShareEmail email = new DatadocShareEmail(datadoc, userFileShare.getShareFrom(), request.getNoteText(), userFileShare.getShareType(), registered);
                mailService.send(sendTo, email);
            } catch (Exception e) {
                log.info("Can not send email to {}. Root cause: {}", userFileShare.getUser().getEmail(), e.getMessage());
            }
        });
        return response;
    }

    @GetMapping("/info/{datadocId}")
    public ShareDatadocInfo retrieveShareDatadocInfo(@PathVariable Long datadocId) {
        return shareService.retrieveShareDatadocInfo(datadocId);
    }

    @GetMapping("/{datadocId}/shared_tabs")
    public List<BookmarkStateViewDTO> getDatadocSharedTabs(@PathVariable Long datadocId) {
        return shareService.getDatadocSharedStates(datadocId);
    }

    @PostMapping("/users_hint")
    public List<User> usersToShareHint(@RequestBody UsersToShareRequest request) {
        return shareService.getShareWithUsers(request);
    }

    @DeleteMapping("/{datadocId}/{userId}")
    public void revokePermissions(@PathVariable Long datadocId, @PathVariable Long userId) {
        shareService.revokePermissions(datadocId, userId);
    }

    @PostMapping("/select_shared_state")
    public void selectSharedState(@RequestBody SelectSharedStateRequest request) {
        shareService.selectSharedState(request.getDatadocId(), request.getSharedStateId());
    }

    @PostMapping("/update")
    public void updateUserPermissions(@RequestBody UpdatePermissionsRequest request) {
        shareService.updateUserPermissions(request);
    }

    @PostMapping("/public")
    public void publicShareTab(@RequestBody PublicShareDatadocRequest shareDatadocRequest) {
        shareService.publicShareDatadoc(shareDatadocRequest);
    }

    @GetMapping("/check/public/{shareId}")
    public ShareDatadocCheckResponse isPublicAccessible(@PathVariable UUID shareId) throws AuthenticationController.NotFoundException {
        Datadoc datadoc = uploadRepository.getDatadocByShareId(shareId);
        if(datadoc == null) {
            throw new AuthenticationController.NotFoundException();
        }
        if(datadoc.getPublicShared()) {
            tableService.initSharedDatadocIfNeeded(datadoc, true, Auth.get().getUserId());
            return new ShareDatadocCheckResponse(ShareType.VIEW, true, false);
        } else {
            return ShareDatadocCheckResponse.NOT_SHARED;
        }
    }

    @GetMapping("/check/{id}")
    public ShareDatadocCheckResponse isSharedDatadoc(@PathVariable Long id) {
        Datadoc datadoc = uploadRepository.getDatadocById(id);

        boolean isOwner = datadoc.getUser().getId().equals(Auth.get().getUserId());
        Map<Long, UserFileShare> shared = datadoc.getSharedWithById();
        UserFileShare userFileShare = shared.get(Auth.get().getUserId());

        if(isOwner) {
            return new ShareDatadocCheckResponse(null, false, true);
        }

        if(userFileShare != null) {
            tableService.initSharedDatadocIfNeeded(datadoc, false, Auth.get().getUserId());
            return new ShareDatadocCheckResponse(userFileShare.getShareType(), true, false);
        }
        throw new ForbiddenException("You do not have access to this datadoc.");
    }


}
