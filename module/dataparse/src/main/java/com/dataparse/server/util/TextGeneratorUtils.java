package com.dataparse.server.util;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TextGeneratorUtils {

    public static String generateString(Integer stringLength, Integer linesCount) {
        List<String> lines = LongStream.range(0, linesCount).boxed().map(operand -> {
            List<String> list = LongStream
                    .range(0, stringLength)
                    .boxed()
                    .map(o -> "" + (char) (new Random().nextInt(60) + 65))
                    .collect(Collectors.toList());
            String[] arr = new String[list.size()];
            return String.join("", list.toArray(arr));
        }).collect(Collectors.toList());
        String[] arrLines = new String[lines.size()];

        return String.join("\n", lines.toArray(arrLines));
    }
}
