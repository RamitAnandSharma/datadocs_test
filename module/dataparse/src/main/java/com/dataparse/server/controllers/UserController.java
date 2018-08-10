package com.dataparse.server.controllers;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.file.SaveAvatarResponse;
import com.dataparse.server.controllers.api.file.SaveFileResponse;
import com.dataparse.server.controllers.api.file.UploadFileRequest;
import com.dataparse.server.controllers.api.user.*;
import com.dataparse.server.service.mail.MailService;
import com.dataparse.server.service.storage.unifersal.PublicStorageInterface;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.service.user.user_state.*;
import com.dataparse.server.service.user.user_state.state.*;
import com.dataparse.server.util.JsonUtils;
import com.github.rholder.retry.*;
import io.swagger.annotations.*;
import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.io.InputStream;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/user")
public class UserController extends ApiController {
//    2mb
    private static final Long IMAGE_MAX_SIZE = 2 * 1024 * 1024L;
    private static final String RESET_PASSWORD_HEADER_TEMPLATE = "Reset Password";
    private static final String RESET_PASSWORD_URL_TEMPLATE = "http://%s/#/reset-password?token=%s";
    private static final String RESET_PASSWORD_BODY_TEMPLATE = "Follow the link: <br/> <a href=\"%url%\">%url%</a>";


    @Autowired
    private PublicStorageInterface publicStorage;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private MailService mailService;

    @Autowired
    private UserStateStorage userStateStorage;

    @RequestMapping(method = RequestMethod.POST, value = "/register")
    public User register(@Valid @RequestBody User user) {
        user.setRegistered(true);
        return userRepository.saveUser(user);
    }

    @PostMapping(value = "/upload_avatar")
    public SaveAvatarResponse uploadFile(HttpServletRequest request,
                                         @RequestHeader(required = false, value = ApiController.API_ARG_NAME) String downloadRequestString) throws Exception {
        ServletFileUpload upload = new ServletFileUpload();
        FileItemIterator iterator = upload.getItemIterator(request);
        if(!iterator.hasNext()) {
            throw new RuntimeException("Image content not found");
        }
        FileItemStream item = iterator.next();
        if(!item.isFormField() && item.getFieldName().equals("file")) {
            UploadFileRequest uploadFileRequest = JsonUtils.readValue(downloadRequestString, UploadFileRequest.class);
            if(uploadFileRequest.getFileSize() > IMAGE_MAX_SIZE) {
                throw new IllegalArgumentException("File size is too big for avatar. Acceptable size " + IMAGE_MAX_SIZE);
            }
            InputStream imageInputString = item.openStream();
            return new SaveAvatarResponse(publicStorage.uploadFile(imageInputString));
        } else {
            throw new RuntimeException("It's not a file.");
        }
    }

    @PostMapping(value = "/change-avatar")
    public User updateUserAvatar(@RequestBody ChangeAvatarRequest request) {
        Long currentUser = Auth.get().getUserId();
        return userRepository.changeAvatar(currentUser, request.getUrl());
    }

    @PostMapping(value = "/update_timezone")
    public User updateUserTimezone(@RequestBody UpdateTimezoneRequest request) {
        Long currentUserId = Auth.get().getUserId();
        boolean isValidTimezone = false;

        String[] validIDs = TimeZone.getAvailableIDs();
        for (String str : validIDs) {
            if (str != null && str.equals(request.getTimezone())) {
                isValidTimezone = true;
                break;
            }
        }

        if (!isValidTimezone) {
            String exceptionMessage = String.format("Failed to parse time zone: %s", request.getTimezone());
            throw new RuntimeException(exceptionMessage);
        }
        TimeZone timezone = TimeZone.getTimeZone(request.getTimezone());
        return userRepository.updateTimezone(currentUserId, timezone);
    }

    @SuppressWarnings("unchecked")
    @RequestMapping(method = RequestMethod.POST, value = "/forgot-password")
    public void forgotPassword(@RequestBody ForgotPasswordRequest request) {
        User user = userRepository.getUserByEmail(request.getEmail());
        if (user == null) {
            throw new RuntimeException("User not found");
        }
        String passwordResetToken = userRepository.generatePasswordResetToken(user.getId());
        String resetPasswordUrl = String.format(RESET_PASSWORD_URL_TEMPLATE, "https://ec2-52-91-2-152.compute-1.amazonaws.com", passwordResetToken);
        Retryer mailRetryer = RetryerBuilder.newBuilder()
                .retryIfException()
                .withWaitStrategy(WaitStrategies.fixedWait(5, TimeUnit.SECONDS))
                .withStopStrategy(StopStrategies.stopAfterAttempt(3))
                .build();
        try {
            mailRetryer.call(() -> {
                String body = RESET_PASSWORD_BODY_TEMPLATE.replace("%url%", resetPasswordUrl);
                mailService.send(user.getEmail(), RESET_PASSWORD_HEADER_TEMPLATE, body, null, null);
                return null;
            });
        } catch (ExecutionException | RetryException e) {
            throw new RuntimeException(e);
        }
    }

    @RequestMapping(method = RequestMethod.POST, value = "/reset-password")
    public User resetPassword(@RequestBody ResetPasswordRequest request) {
        User user = userRepository.getUserByPasswordResetToken(request.getToken());
        if (user == null) {
            throw new RuntimeException("Invalid password reset token");
        }
        if (new DateTime(user.getPasswordResetDate()).plusDays(1).isBeforeNow()) {
            throw new RuntimeException("Password reset token expired");
        }
        return userRepository.resetPassword(user.getId(), request.getPassword());
    }

    @ApiOperation(value = "Return user state")
    @RequestMapping(value = "/state/{id}", method = RequestMethod.GET)
    public UserState getUserState(@PathVariable Long id){
        return userStateStorage.get(id);
    }
}
