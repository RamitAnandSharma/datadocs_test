package com.dataparse.server.controllers.api.file;

import lombok.*;
import lombok.experimental.Builder;

import java.util.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeleteRequest {
    List<String> paths;
}
