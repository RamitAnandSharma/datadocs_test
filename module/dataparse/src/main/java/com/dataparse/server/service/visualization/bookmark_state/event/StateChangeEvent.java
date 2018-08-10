package com.dataparse.server.service.visualization.bookmark_state.event;

import com.dataparse.server.service.notification.*;
import com.dataparse.server.websocket.*;
import com.fasterxml.jackson.annotation.*;
import lombok.*;
import org.springframework.beans.factory.annotation.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@type")
public abstract class StateChangeEvent<T> extends Event {

    @Autowired
    SockJSService sockJSService;

    String id;

    @JsonIgnore
    protected StateChangeEvent parentEvent;

    public abstract void apply(T state);

    public boolean validate(){
        return true; // todo on validate drop other events and send error to client
    }
}
