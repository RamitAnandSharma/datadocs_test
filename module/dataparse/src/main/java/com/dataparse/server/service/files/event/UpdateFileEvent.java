package com.dataparse.server.service.files.event;

import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.notification.Event;
import com.dataparse.server.service.notification.EventType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UpdateFileEvent extends Event {

    private AbstractFile file;
    private AbstractFile oldFile;

    @Override
    public EventType getType() {
        return EventType.EDIT_FILE;
    }
}
