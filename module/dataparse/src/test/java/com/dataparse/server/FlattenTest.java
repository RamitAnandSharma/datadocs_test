package com.dataparse.server;

import com.dataparse.server.util.Flatten;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

@Slf4j
public class FlattenTest {
    ObjectMapper mapper = new ObjectMapper();

    @Test
    public void flattenObject() {
        Map<String, Object> test = ImmutableMap.of("g", ImmutableMap.of("i", "j"));

        Map<String, Object> tuples = Flatten.flatten(test);
        log.info("FLATTEN: {}", tuples);

        Map<String, Object> origin = Flatten.unflatten(tuples);
        log.info("UNFLATTEN: {}", origin);
    }


    @Test
    public void flattenObjectWithArrays() {
        Map<String, Object> test = ImmutableMap.of("g", Arrays.asList(1, "h", ImmutableMap.of("i", "j")));

        Map<String, Object> tuples = Flatten.flatten(test);
        log.info("FLATTEN: {}", tuples);

        Map<String, Object> origin = Flatten.unflatten(tuples);
        log.info("UNFLATTEN: {}", origin);
    }

    @Test
    public void flattenNestedArrays() {
        Map<String, Object> test = ImmutableMap.of("a", Arrays.asList("g", Arrays.asList(1, "h", Arrays.asList("i", "j"))));

        Map<String, Object> tuples = Flatten.flatten(test);
        log.info("FLATTEN: {}", tuples);

        Map<String, Object> origin = Flatten.unflatten(tuples);
        log.info("UNFLATTEN: {}", origin);
    }

    @Test
    public void flattenDepth() throws IOException {
        Map<String, Object> map = mapper.readValue(this.getClass().getClassLoader().getResourceAsStream("json_depth_test.json"), Map.class);
        Map<String, Object> tuples = Flatten.flatten(map);
        Assert.assertEquals(tuples.size(), 5);
    }

}

