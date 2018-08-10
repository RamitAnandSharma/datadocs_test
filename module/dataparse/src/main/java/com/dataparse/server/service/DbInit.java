package com.dataparse.server.service;

import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;

@Service
public class DbInit {

    @Autowired
    private UserRepository userRepository;

    @PostConstruct
    public void init() {
        initUsers();
    }

    private List<InitialUser> initialUsers = Arrays.asList(
            new InitialUser("Quincy Quin", "q", "q"),
            new InitialUser("Wagner Wag", "w", "w"),
            new InitialUser("Evan Ev", "e", "e"),
            new InitialUser("Ricky Ric", "r", "r"),
            new InitialUser("Tomas To", "t", "t"),
            new InitialUser("You You", "y", "y"),
            new InitialUser("Vlad Koval", "vladislav1koval@gmail.com", "test"),
            new InitialUser("Konstantin Motorniy", "motorniy.freshcode@gmail.com", "test"),
            new InitialUser("David Litwin", "deemarklit@gmail.com", "test"),
            new InitialUser("Kyle Hall", "kyle@cinely.com", "test")
    );

    private void initUsers() {
        initialUsers.forEach(initialUser -> {
            User userByEmail = userRepository.getUserByEmail(initialUser.getEmail());
            if (userByEmail != null) {
                userByEmail.setFullName(initialUser.getFullName());
                userByEmail.setPassword(initialUser.getPassword());
                userByEmail.setAdmin(initialUser.isAdmin());
                userRepository.updateUser(userByEmail);
            } else {
                User user = new User(initialUser.getEmail(), initialUser.getPassword());
                user.setFullName(initialUser.getFullName());
                user.setRegistered(true);
                user.setAdmin(initialUser.isAdmin()); // true by default
                // Can be passed as the fourth parameter for users in the list
                userRepository.saveUser(user);
            }
        });
    }
}
