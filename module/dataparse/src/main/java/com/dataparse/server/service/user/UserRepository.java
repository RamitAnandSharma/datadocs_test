package com.dataparse.server.service.user;

import com.dataparse.server.service.user.user_state.UserStateBuilder;
import com.dataparse.server.service.user.user_state.UserStateStorage;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import org.mindrot.jbcrypt.BCrypt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

@Repository
public class UserRepository {

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private UserStateStorage userStateStorage;

    @Autowired
    private UserStateBuilder userStateBuilder;


    @Transactional
    public User getUserByEmail(String email) {
        return (User) sessionFactory.getCurrentSession()
                .createCriteria(User.class)
                .add(Restrictions.eq("email", email).ignoreCase())
                .uniqueResult();
    }

    @Transactional
    public User getUserByPasswordResetToken(String token) {
        return (User) sessionFactory.getCurrentSession()
                .createCriteria(User.class)
                .add(Restrictions.eq("passwordResetToken", token))
                .uniqueResult();
    }

    @Transactional
    public User getUser(Long id) {
        return (User) sessionFactory.getCurrentSession()
                .get(User.class, id);
    }

    @Transactional
    public User saveUser(User user) {
        User userByEmail = getUserByEmail(user.getEmail());
        if (userByEmail != null && userByEmail.getRegistered()) {
            throw new RuntimeException("Address \"" + user.getEmail() + "\" already in use");
        }
        String passwordHash = BCrypt.hashpw(user.getPassword(), BCrypt.gensalt());
        user.setPassword(passwordHash);

        //        pre registered user
        if(userByEmail != null) {
            user.setId(userByEmail.getId());
            sessionFactory.getCurrentSession().detach(userByEmail);
        }

        sessionFactory.getCurrentSession().saveOrUpdate(user);
        userStateStorage.init(userStateBuilder.create(user.getId()));
        return user;
    }

    @Transactional
    public User updateUser(User user) {
        String passwordHash = BCrypt.hashpw(user.getPassword(), BCrypt.gensalt());
        user.setPassword(passwordHash);

        sessionFactory.getCurrentSession().update(user);
        return user;
    }

    @Transactional
    public User saveNotRegisteredUser(User user) {
        User userByEmail = getUserByEmail(user.getEmail());
        if (userByEmail == null || !userByEmail.getRegistered()) {
            sessionFactory.getCurrentSession().save(user);
            return user;
        }
        throw new IllegalStateException("User already registered. ");
    }



    @Transactional
    public User resetPassword(Long userId, String password){
        User user = getUser(userId);
        user.setPassword(BCrypt.hashpw(password, BCrypt.gensalt()));
        user.setPasswordResetDate(null);
        user.setPasswordResetToken(null);
        sessionFactory.getCurrentSession().update(user);
        return user;
    }

    @Transactional
    public String generatePasswordResetToken(Long userId){
        User user = getUser(userId);
        user.setPasswordResetToken(UUID.randomUUID().toString());
        user.setPasswordResetDate(new Date());
        sessionFactory.getCurrentSession().update(user);
        return user.getPasswordResetToken();
    }

    @Transactional
    public User changeAvatar(Long userId, String avatarPath) {
        User user = getUser(userId);
        user.setAvatarPath(avatarPath);
        sessionFactory.getCurrentSession().update(user);
        return user;
    }

    @Transactional
    public User updateTimezone(Long userId, TimeZone timezone) {
        User user = getUser(userId);
        user.setTimezone(timezone);
        sessionFactory.getCurrentSession().update(user);
        return user;
    }

}
