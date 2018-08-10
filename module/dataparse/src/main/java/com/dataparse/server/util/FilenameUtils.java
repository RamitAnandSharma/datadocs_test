package com.dataparse.server.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FilenameUtils {

    public static final List<String> FORBIDDEN_CHARS = Arrays.asList("<", ">", "\\", "/", ":", "?", "*", "\"", "|");

    public static final String PATH_ID_PREFIX = "id:";
    public static final String PATH_SEPARATOR = "/";

    public static boolean isPath(String pathString){
        return !(!pathString.isEmpty() && !pathString.startsWith(PATH_SEPARATOR) && !pathString.startsWith(PATH_ID_PREFIX));
    }

    public static void validatePath(String pathString){
        pathString = pathString.trim();
        if(!isPath(pathString)){
            throw new RuntimeException("Path must start with a separator (\"" + PATH_SEPARATOR + "\") or ID prefix (\"" + PATH_ID_PREFIX +"\")");
        }
    }

    public static void validateFilename(String name){
        for(String character : FilenameUtils.FORBIDDEN_CHARS){
            if(name.contains(character)){
                throw new RuntimeException("The following characters are not allowed: " + String.join(" ", FORBIDDEN_CHARS));
            }
        }
    }

    public static String getPath(String path){
        validatePath(path);

        if(path.startsWith(PATH_ID_PREFIX) && !path.contains(PATH_SEPARATOR)){
            return path;
        }

        int separatorIdx = path.lastIndexOf(PATH_SEPARATOR);
        if(separatorIdx < 0){
            return "";
        }
        return path.substring(0, separatorIdx);
    }

    public static String getName(String path){
        validatePath(path);

        if(path.startsWith(PATH_ID_PREFIX) && !path.contains(PATH_SEPARATOR)){
            return "";
        }

        int separatorIdx = path.lastIndexOf(PATH_SEPARATOR);
        if(separatorIdx < 0){
            return path;
        }
        return path.substring(separatorIdx + 1);
    }

    public static List<String> getParentFolders(String path){
        String tmp = getPath(path);
        if(tmp.trim().isEmpty()){
            return Collections.emptyList();
        }
        while(tmp.startsWith(PATH_SEPARATOR)){
            tmp = tmp.substring(1);
        }
        return Arrays.asList(tmp.split("/+"));
    }
}
