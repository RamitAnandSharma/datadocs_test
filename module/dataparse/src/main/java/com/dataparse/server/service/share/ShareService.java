package com.dataparse.server.service.share;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.share.*;
import com.dataparse.server.service.cache.ICacheEvictionService;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.files.ShareType;
import com.dataparse.server.service.files.UserFileShare;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.upload.UploadRepository;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.service.visualization.bookmark_state.dto.BookmarkStateViewDTO;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkStateRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class ShareService {
    @Autowired
    private ShareRepository shareRepository;

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private BookmarkStateRepository bookmarkStateRepository;

    @Autowired
    private UploadRepository uploadRepository;

    @Autowired
    private ICacheEvictionService cacheEvictionService;

    public UserFileShare shareDatadoc(Datadoc datadoc, String email, ShareType shareType, Boolean shareAttachedSources) {
        User shareTo = userRepository.getUserByEmail(email);
        User shareFrom = userRepository.getUser(Auth.get().getUserId());

        if(shareTo == null) {
            User mockUser = new User();
            mockUser.setRegistered(false);
            mockUser.setEmail(email);
            shareTo = userRepository.saveNotRegisteredUser(mockUser);
        }

        if(datadoc.getUser().getEmail().equals(email)) {
            throw new IllegalStateException("You are trying to share with owner.");
        }

        List<TableBookmark> tableBookmarks = tableRepository.getTableBookmarks(datadoc.getId());
        tableBookmarks.forEach(tableBookmark -> bookmarkStateRepository.getAllBookmarkStates(tableBookmark.getId(), shareFrom.getId())
                .forEach(state -> {
                    state.setShared(true);
                    bookmarkStateRepository.save(state);
                }));

        if (shareAttachedSources) {
            List<Upload> uploads = tableRepository.getAttachedSources(datadoc.getId());
            for (Upload upload : uploads) {
                Upload clone = upload.copy();
                clone.setUser(shareTo);
                uploadRepository.saveFile(clone);
            }
        }

        checkAlreadySharedDatadoc(datadoc, email);
        checkPermissionToShare(datadoc, shareFrom, email);
        UserFileShare shareFile = new UserFileShare(shareTo, datadoc, shareType, shareFrom, datadoc.getUser());
        shareRepository.saveShareDatadoc(shareFile);
        cacheEvictionService.findKeyByPartialKey(datadoc.getId().toString() + shareFrom.getId().toString())
                .forEach(cachedKey -> cacheEvictionService.evict(cachedKey));
        return shareFile;
    }

    public ShareDatadocResponse shareDatadoc(Datadoc datadoc, ShareDatadocRequest request) {
        ShareDatadocResponse shareDatadocResponse = new ShareDatadocResponse();
        request.getEmail().forEach(email -> {
            try {
                UserFileShare sharedFile = shareDatadoc(datadoc, email, request.getShareType(), request.getShareAttachedSources());
                shareDatadocResponse.addSuccess(sharedFile);
            } catch (Exception e) {
                shareDatadocResponse.addFailed(e.getMessage());
            }
        });
        return shareDatadocResponse;
    }

    public void selectSharedState(Long datadocId, UUID stateId) {
        Datadoc datadoc = tableRepository.getDatadoc(datadocId);
        datadoc.setSharedStateId(stateId);
        tableRepository.updateDatadoc(datadoc);
    }

    public ShareDatadocInfo retrieveShareDatadocInfo(Long datadocId) {
        Datadoc datadoc = tableRepository.getDatadoc(datadocId);
        User datadocOwner = datadoc.getUser();
        List<UserFileShare> sharedWith = retrieveSharedWith(datadoc);
        ShareType shareType = defineShareType(datadocId, datadoc);
        return new ShareDatadocInfo(
                sharedWith,
                datadocOwner,
                shareType,
                datadoc.getPublicShared(),
                datadoc.getShareId(),
                datadoc.getSharedStateId());
    }

    private List<UserFileShare> retrieveSharedWith(Datadoc datadoc) {
        return shareRepository.retrieveSharedUsers(datadoc);
    }

    private ShareType defineShareType(Long datadocId, Datadoc datadoc) {
        boolean isOwner = datadoc.getUserId().equals(Auth.get().getUserId());
        ShareType shareType = shareRepository.getShareType(datadocId, Auth.get().getUserId());

        if(isOwner) {
            return ShareType.ADMIN;
        } else if (shareType != null) {
            return shareType;
        } else if (datadoc.getPublicShared()) {
            return ShareType.VIEW;
        }
        return null;
    }

    public void publicShareDatadoc(PublicShareDatadocRequest request) {
        Datadoc datadoc = tableRepository.getDatadoc(request.getDatadocId());
        if(!datadoc.getPublicShared().equals(request.getEnable())) {
//            nothing changed here
            datadoc.setPublicShared(request.getEnable());
            tableRepository.updateDatadoc(datadoc);
        }
    }

    public void cleanUpSharing(Long datadocId) {
        shareRepository.cleanUpSharing(datadocId);
    }

    public List<User> getShareWithUsers(UsersToShareRequest request) {
        return shareRepository.getShareWithUsers(request.getDatadocId(), Auth.get().getUserId(), request.getNamePart());
    }

    public List<BookmarkStateViewDTO> getDatadocSharedStates(Long datadocId) {
        return tableRepository.getDatadocSharedStates(datadocId);
    }

    public void revokePermissions(Long datadocId, Long userId) {
        shareRepository.revokePermissions(datadocId, userId);
    }

    public void updateUserPermissions(UpdatePermissionsRequest request) {
        UserFileShare sharedInfo = shareRepository.getSharedInfo(request.getDatadocId(), request.getUserId());
        if(sharedInfo != null) {
            sharedInfo.setShareType(request.getShareType());
            shareRepository.updatePermissions(sharedInfo);
        }
    }

    private void checkAlreadySharedDatadoc(Datadoc datadoc, String email) {
        Optional<User> alreadyShared = datadoc.getSharedWith().stream().filter(user -> email.equals(user.getEmail())).findFirst();
        if(alreadyShared.isPresent()) {
            throw new RuntimeException(String.format("Already shared with user '%s'", email));
        }
    }

    private void checkPermissionToShare(Datadoc datadoc, User shareFrom, String shareToEmail) {
        boolean isOwner = shareFrom.equals(datadoc.getUser());
        UserFileShare sharedInfo = shareRepository.getSharedInfo(datadoc.getId(), shareFrom.getId());

        boolean isAllowedByShareHierarchy = sharedInfo != null && sharedInfo.getShareType().isAdmin();

        if(!isOwner && !isAllowedByShareHierarchy) {
            throw new RuntimeException(String.format("You don't have enough permissions to share this datadoc with \"%s\"", shareToEmail));
        }
    }

}
