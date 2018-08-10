package com.dataparse.server.service.user.user_state.state;

import com.dataparse.server.service.AbstractMongoRepository;
import lombok.extern.slf4j.Slf4j;
import org.mongojack.DBQuery;
import org.mongojack.JacksonDBCollection;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.UnknownHostException;

@Slf4j
@Service
public class UserStateRepository extends AbstractMongoRepository {

    private JacksonDBCollection<UserState, Long> userStateCollection;

    @PostConstruct
    public void init() throws UnknownHostException {
        super.init();
        userStateCollection = JacksonDBCollection.wrap(
                database.getCollection("user_state"), UserState.class, Long.class);
    }

    public UserState get(Long userId){
        return userStateCollection.findOneById(userId);
    }

    public void save(UserState userState) {
        userStateCollection.update(DBQuery.is("_id", userState.getUserId()), userState, true, false);
    }

    public void remove(UserState userState) {
        userStateCollection.removeById(userState.getUserId());
    }

}
