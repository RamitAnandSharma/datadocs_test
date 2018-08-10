package com.dataparse.server.service.docs;

import com.dataparse.server.service.notification.Event;
import com.dataparse.server.service.notification.EventType;
import com.dataparse.server.service.user.UserDTO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DatadocViewedEvent extends Event {

    private ArrayList<UserDTO> datadocViewUsers;

    @Override
    public EventType getType() {
        return EventType.DATADOC_VIEWED;
    }
}
