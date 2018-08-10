package com.dataparse.server.service.flow.convert;

import com.dataparse.server.service.tasks.*;
import lombok.*;

@Data
public class ConvertTaskResult extends TaskResult {

    private Long progress;
    private String path;

}
