package com.dataparse.server.service.docs;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserDTO;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.websocket.IDatadocNotificationService;
import com.dataparse.server.websocket.SockJSService;
import com.dataparse.server.websocket.UnsubscribeUserInterface;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;


import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import java.util.*;
import java.util.stream.Collectors;

import lombok.extern.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DatadocNotificationService implements IDatadocNotificationService, UnsubscribeUserInterface {

    private Multimap<Long, UserDTO> connectedToDatadocUsers = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private SockJSService sockJSService;

    @Getter
    private static String datadocEventTopic = "/doc/event/";

    private void notifyAboutConnectionListChange(Long datadocId) {
        ArrayList<UserDTO> datadocViewers = new ArrayList<>(connectedToDatadocUsers.get(datadocId));

        sockJSService.send(Auth.get(), datadocEventTopic + datadocId,
                new DatadocViewedEvent(datadocViewers), true);
    }

    private void notifyAboutDatadocMetaChange(DatadocEvent event) {
        sockJSService.send(Auth.get(), datadocEventTopic + event.getDatadocId(), event, true);
    }

    public void handleEvent(DatadocEvent event) {
        Long datadocId = event.getDatadocId();
        Long tabId = event.getTabId();
        String renameTo = event.getRenameTo();
        Integer tabIndex = event.getTabIndex();
        UUID stateId = event.getStateId();

        switch (event.getType()) {
            case DATADOC_NAME_CHANGED:
                notifyAboutDatadocMetaChange(new DatadocNameChangedEvent(datadocId, renameTo));
                break;
            case TAB_NAME_CHANGED:
                notifyAboutDatadocMetaChange(new TabNameChangedEvent(datadocId, tabId, renameTo));
                break;
            case VIEW_DATADOC:
                addToViewers(new ViewDatadocEvent(datadocId));
                break;
            case TAB_ADDED:
                notifyAboutDatadocMetaChange(new TabAddedEvent(datadocId, tabId, stateId));
                break;
            case TAB_REMOVED:
                notifyAboutDatadocMetaChange(new TabRemovedEvent(datadocId, tabId, tabIndex));
                break;
            case TAB_POSITION_CHANGED:
                notifyAboutDatadocMetaChange(new TabPositionChangedEvent(datadocId, tabId, tabIndex));
                break;
        }
    }

    public void addToViewers(ViewDatadocEvent event) {
        Long datadocId = event.getDatadocId();
        Long userId = Auth.get().getUserId();

        User user = userRepository.getUser(userId);
        UserDTO userDto = new UserDTO(user);

        if (userId != null) {
            connectedToDatadocUsers.get(datadocId).add(userDto);
        }

        notifyAboutConnectionListChange(datadocId);
    }

    public void removeUser(Auth auth) {
        if (auth.getUserId() != null) {
            List<Pair<Long, Long>> usersForRemove = connectedToDatadocUsers.keySet()
                    .stream()
                    .map(datadocId -> connectedToDatadocUsers.get(datadocId).stream().anyMatch(u -> u.getUserId().equals(auth.getUserId()))
                            ? Pair.of(datadocId, auth.getUserId())
                            : null)
                    .filter(Objects::nonNull).collect(Collectors.toList());

            usersForRemove.forEach(forRemove -> {
                connectedToDatadocUsers.get(forRemove.getKey()).removeIf(u -> u.getUserId().equals(forRemove.getValue()));
                notifyAboutConnectionListChange(forRemove.getKey());
            });
        }
    }

    // TODO: Redo this "Conceptual trash" (1)
    public void addUser(Auth auth) {}

    public void unsubscribeUser(Auth auth) {
        removeUser(auth);
    }
}
