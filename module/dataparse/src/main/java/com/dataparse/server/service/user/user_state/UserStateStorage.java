package com.dataparse.server.service.user.user_state;

import com.dataparse.server.service.user.user_state.event.*;
import com.dataparse.server.service.user.user_state.state.Section;
import com.dataparse.server.service.user.user_state.state.UserState;
import com.dataparse.server.service.user.user_state.state.UserStateRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Slf4j
@Service
public class UserStateStorage {

    @Autowired
    private UserStateRepository userStateRepository;

    public void init(Long userId){
        userStateRepository.save(new UserState(userId));
    }

    public void init(UserState state){
        userStateRepository.save(state);
    }

    public void reset(Long userId) {
        UserState state = userStateRepository.get(userId);
        state.setSelectedFolderId(null);
        state.setActiveSection(Section.MY_DATA);
        userStateRepository.save(state);
    }

    public UserState get(Long userId){
        return userStateRepository.get(userId);
    }

    public void accept(UserStateChangeEvent event){
        Long userId = event.getUserId();
        UserState state = userStateRepository.get(userId);
        event.apply(state);
        userStateRepository.save(state);
    }

}

