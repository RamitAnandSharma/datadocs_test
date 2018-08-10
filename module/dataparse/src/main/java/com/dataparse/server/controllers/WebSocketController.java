package com.dataparse.server.controllers;

import com.dataparse.server.service.docs.DatadocEvent;
import com.dataparse.server.service.user.user_state.*;
import com.dataparse.server.service.user.user_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.*;
import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.docs.DatadocNotificationService;
import lombok.extern.slf4j.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.messaging.handler.annotation.*;
import org.springframework.stereotype.*;

@Slf4j
@Controller
public class WebSocketController {

    @Autowired
    private BookmarkStateStorage bookmarkStateStorage;

    @Autowired
    private UserStateStorage userStateStorage;

    @Autowired
    private DatadocNotificationService DatadocNotificationService;

    @MessageMapping("/vis/event" )
    public void handle(BookmarkStateChangeEvent event) {
        bookmarkStateStorage.add(event);
    }

    @MessageMapping("/user/event" )
    public void handle(UserStateChangeEvent event) {
        userStateStorage.accept(event);
    }

    @MessageMapping("/doc/event")
    public void handle(DatadocEvent event) {
        DatadocNotificationService.handleEvent(event);
    }
}
