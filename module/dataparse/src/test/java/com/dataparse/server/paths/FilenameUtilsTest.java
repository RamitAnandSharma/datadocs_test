package com.dataparse.server.paths;

import com.dataparse.server.util.FilenameUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class FilenameUtilsTest {

    private static boolean isValidationOk(Runnable r){
        try {
            r.run();
            return true;
        } catch (Exception e){
            return false;
        }
    }

    @Test
    public void validationTest(){
        assertTrue(isValidationOk(() -> FilenameUtils.validatePath("")));
        assertTrue(isValidationOk(() -> FilenameUtils.validatePath("/")));
        assertTrue(isValidationOk(() -> FilenameUtils.validatePath("/home")));
        assertTrue(isValidationOk(() -> FilenameUtils.validatePath("/home/user")));
        assertTrue(isValidationOk(() -> FilenameUtils.validatePath("id:1")));
        assertTrue(isValidationOk(() -> FilenameUtils.validatePath("id:1/user")));
        assertTrue(isValidationOk(() -> FilenameUtils.validatePath("/1/")));

        assertFalse(isValidationOk(() -> FilenameUtils.validatePath("123")));
    }

    @Test
    public void parseFilenameTest(){
        assertEquals("name.txt", FilenameUtils.getName("/home/user/name.txt"));
        assertEquals("name.txt", FilenameUtils.getName("/name.txt"));
        assertEquals("name", FilenameUtils.getName("/name.txt/name"));
    }

    @Test
    public void parseFilePathTest(){
        assertEquals("/home/user", FilenameUtils.getPath("/home/user/name.txt"));
        assertEquals("", FilenameUtils.getPath("/name.txt"));
        assertEquals("/name.txt", FilenameUtils.getPath("/name.txt/name"));
    }

    @Test
    public void getParentFolders(){
        assertEquals(Collections.emptyList(), FilenameUtils.getParentFolders(""));
        assertEquals(Collections.emptyList(), FilenameUtils.getParentFolders("/"));
        assertEquals(Collections.emptyList(), FilenameUtils.getParentFolders("/name.txt"));
        assertEquals(Arrays.asList("home", "user"), FilenameUtils.getParentFolders("/home/user/name.txt"));
        assertEquals(Arrays.asList("home", "user"), FilenameUtils.getParentFolders("////home///user///////name.txt"));

    }

}
