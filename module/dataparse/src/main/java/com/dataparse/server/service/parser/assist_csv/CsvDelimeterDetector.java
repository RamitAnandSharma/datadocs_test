package com.dataparse.server.service.parser.assist_csv;

import com.dataparse.server.service.parser.CSVParser;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.util.InputStreamUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;


public class CsvDelimeterDetector extends AbstractCsvDetector{

    public CsvDelimeterDetector(FileStorage fs, String path) {
        super(fs, path);
    }

    public Pair<Character, String> tryGetDelimiter(long offset, String charsetName) throws IOException {
        try(InputStream is = fileStorage.getFile(path)) {
            is.skip(offset);
            return tryGetDelimiter(is, charsetName);
        }
    }

    public Pair<Character, String> tryGetDelimiter(InputStream is, String charsetName) throws IOException
    {
        String buffer;
        try(InputStream noBomIs = InputStreamUtils.wrapWithoutBOM(is);
            BoundedInputStream bis = new BoundedInputStream(noBomIs, 102400)){
            buffer = IOUtils.toString(bis, charsetName);
        }
        for(String rowDelimiter: CSVParser.ROW_DELIMITERS) {
            try (Scanner scanner = new Scanner(buffer)) {
                // at least 2 lines: header(or content) + content, should contain same amount of delimiters

                List<String> lines = new ArrayList<>();
                String line1 = getNextNonCommented(scanner, rowDelimiter);
                if (StringUtils.isBlank(line1)) {
                    continue;
                }
                String line2 = getNextNonCommented(scanner, rowDelimiter);
                if (StringUtils.isBlank(line2)) {
                    continue;
                }
                lines.add(line1);
                lines.add(line2);
                String newLine;
                int additionalLines = 3;
                while(StringUtils.isNotBlank(newLine = getNextNonCommented(scanner, rowDelimiter))
                        && additionalLines-- > 0){
                    lines.add(newLine);
                }
                List<Map<Character, Integer>> delimiterCounts = lines.stream()
                        .map(line -> countDelimiterMatches(line))
                        .collect(Collectors.toList());

                Optional<Character> recordDelimiter = CSVParser.DELIMITERS.stream().filter(delimiter -> {
                    List<Integer> counts = delimiterCounts.stream()
                            .map(dc -> dc.get(delimiter))
                            .collect(Collectors.toList());
                    return counts.get(0) > 0 && counts.stream().distinct().count() == 1;
                }).max((o1, o2) -> {
                    Map<Character, Integer> countsOnFirstLine = delimiterCounts.get(0);
                    return countsOnFirstLine.get(o1).compareTo(countsOnFirstLine.get(o2));
                });
                if(recordDelimiter.isPresent()){
                    return Pair.of(recordDelimiter.get(), rowDelimiter);
                }
            }
        }
        return Pair.of(CSVParser.DELIMITERS.get(0), CSVParser.DEFAULT_ROW_DELIMITER);
    }

    // ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

    private static Map<Character, Integer> countDelimiterMatches(String line){
        return CSVParser.DELIMITERS.stream().collect(toMap(Function.identity(), d -> {
            boolean screened = false;
            int count = 0;
            for(int i = 0; i<line.length(); i++){
                if(line.charAt(i) == '\"'){
                    screened = !screened;
                } else {
                    if(!screened){
                        if(line.charAt(i) == d){
                            count++;
                        }
                    }
                }
            }
            return count;
        }));
    }

    private String getNextNonCommented(Scanner scanner, String delimiter) throws IOException {
        scanner.useDelimiter(delimiter);
        String line;
        do{
            if(scanner.hasNext()) {
                line = scanner.next();
                if (line == null) {
                    return null;
                }
            } else {
                return null;
            }
        } while (isCommented(line));
        return line;
    }

    private boolean isCommented(String line){
        return line.trim().startsWith(CSVParser.COMMENT_CHARACTER_DEFAULT.toString());
    }

}

