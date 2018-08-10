package com.dataparse.server.controllers;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.engine.*;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.service.user.user_state.UserStateBuilder;
import com.dataparse.server.service.user.user_state.UserStateStorage;
import com.dataparse.server.service.user.user_state.state.UserState;
import org.mindrot.jbcrypt.BCrypt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/auth")
public class AuthenticationController extends ApiController {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private UserStateStorage userStateStorage;

    @Autowired
    private UserStateBuilder userStateBuilder;

    private boolean matchesPassword(User user, String password) {
        return BCrypt.checkpw(password, user.getPassword());
    }

    @ResponseStatus(value = HttpStatus.UNAUTHORIZED)
    public static class UnauthorizedException extends Exception {

    }

    @ResponseStatus(value = HttpStatus.NOT_FOUND)
    public static class NotFoundException extends Exception {

    }

    @RequestMapping(value = "/login", method = RequestMethod.POST)
    public User login(@RequestParam("login") String login, @RequestParam("password") String password, @RequestParam(value = "anon", required = false) Boolean anon) throws UnauthorizedException, NotFoundException {
        User user = userRepository.getUserByEmail(login);
        // check password
        if (user == null) {
            throw new NotFoundException();
        } else {
            user.setManualEngineSelection(EngineSelectionStrategy.isAllowManualSelection());
            UserState userState = userStateStorage.get(user.getId());
            if (userState == null) {
                userStateStorage.init(userStateBuilder.create(user.getId()));
            }

            if ((anon != null && anon) || matchesPassword(user, password)) {
                Auth.setCurrentUser(user.getId());
                user.setSessionId(Auth.get().getSessionId());
                return user;
            } else {
                throw new UnauthorizedException();
            }
        }
    }

    @RequestMapping(value = "/logout", method = RequestMethod.POST)
    public void logout() {
        if(Auth.get().getUserId() != null) {
            userStateStorage.reset(Auth.get().getUserId());
            Auth.removeCurrentUser();
        }
    }

    @RequestMapping(value = "current", method = RequestMethod.GET)
    public User getCurrentUser() {
        User user = userRepository.getUser(Auth.get().getUserId());
        user.setManualEngineSelection(EngineSelectionStrategy.isAllowManualSelection());
        return user;
    }

}
