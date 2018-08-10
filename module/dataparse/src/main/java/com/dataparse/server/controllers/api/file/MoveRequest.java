package com.dataparse.server.controllers.api.file;

import lombok.Data;

import java.util.*;

@Data
public class MoveRequest {
    List<String> fromPaths;
    String toPath;
}
