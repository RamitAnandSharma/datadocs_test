package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.IndexedParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.XlsFileDescriptor;
import com.dataparse.server.util.ExceptionUtils;
import com.dataparse.server.util.FunctionUtils;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Time;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.dataparse.server.service.parser.RecordIteratorBuilder.with;
import static java.util.Arrays.asList;

@Slf4j
public abstract class AbstractXlsParser extends Parser {

    private static final String STRIP_CHARACTERS = "'`";

    private Integer ROWS_COUNT_FOR_HEADER = 10;
    private Set<String> POSSIBLE_COMMENT_SIGNS = Sets.newHashSet("//");

    private String sheetName;
    private Workbook workbook;
    private Sheet currentSheet;
    private FormulaEvaluator evaluator;

    private boolean useHeaders;
    private int startOnRow;
    private int skipAfterHeader;
    private int skipFromBottom;

    private Descriptor descriptor;

    public AbstractXlsParser(FileStorage fileStorage, XlsFileDescriptor descriptor) {
        this.useHeaders = Optional.ofNullable(descriptor.getSettings().getUseHeaders()).orElse(true);
        this.startOnRow = Optional.ofNullable(descriptor.getSettings().getStartOnRow()).orElse(1);
        if(this.startOnRow < 1 || this.startOnRow > 65535){
            throw new RuntimeException("Start On Row should be positive integer less than 65535");
        }
        this.skipAfterHeader = Optional.ofNullable(descriptor.getSettings().getSkipAfterHeader()).orElse(0);
        if(this.skipAfterHeader < 0 || this.skipAfterHeader > 65535){
            throw new RuntimeException("Skip After Header should be positive integer less than 65535");
        }
        this.skipFromBottom = Optional.ofNullable(descriptor.getSettings().getSkipFromBottom()).orElse(0);
        if(this.skipFromBottom < 0 || this.skipFromBottom > 65535){
            throw new RuntimeException("Skip From Bottom should be positive integer less than 65535");
        }
        this.descriptor = descriptor;
        try(InputStream is = fileStorage.getFile(descriptor.getPath())) {
            workbook = getWorkbook(is);
        } catch (IOException e) {
            throw new RuntimeException();
        }
        evaluator = workbook.getCreationHelper().createFormulaEvaluator();
        setCurrentSheet(descriptor.getSheetName());
    }

    @Override
    public Pair<Long, Boolean> getRowsEstimateCount(long fileSize) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Pair<Long, Boolean> result = Pair.of(Math.max(0, (long) (currentSheet.getLastRowNum() - getFirstRowNum(currentSheet) - (skipFromBottom + skipAfterHeader))), true);
        log.info("Define estimate rows count took {}", stopwatch);
        return result;
    }

    public void setCurrentSheet(final String sheetName){
        this.sheetName = sheetName;
        if(!isSheetSpecified()) {
            currentSheet = workbook.getSheetAt(0);
        } else {
            currentSheet = workbook.getSheet(sheetName);
        }
    }

    @Override
    public void close() throws IOException {
        currentSheet.getWorkbook().close();
    }

    private boolean isSheetSpecified(){
        return StringUtils.isNotBlank(sheetName);
    }

    public List<String> getSheets() throws IOException {
        List<String> result = new ArrayList<>();
        int sheets = workbook.getNumberOfSheets();
        for (int i = 0; i < sheets; i++) {
            Sheet sheet = workbook.getSheetAt(i);
            List<ColumnInfo> headers = getHeaders(sheet).values().stream()
                    .map(h -> {
                        ColumnInfo column = new ColumnInfo();
                        column.setName(h);
                        return column;
                    })
                    .collect(Collectors.toList());
            if(!headers.isEmpty()) {
                result.add(sheet.getSheetName());
            }
        }
        return result;
    }

    private int getFirstRowNum(Sheet sheet){
        return sheet.getFirstRowNum() + (this.startOnRow - 1);
    }

    @Override
    public Integer getMorePossibleHeaderIndex() {
        int firstRow = getFirstRowNum(currentSheet);
        List<Row> rowsForProcess = IntStream.range(firstRow, ROWS_COUNT_FOR_HEADER)
                .boxed()
                .map(currentSheet::getRow)
                .collect(Collectors.toList());
        List<Map<Integer, String>> possibleHeaders = getRowsValues(rowsForProcess);
        return defineHeader(possibleHeaders);
    }

    @Override
    public Integer getSkipRowsCountAfterHeader(Integer headerRow) {
        List<Row> rowsForProcess = IntStream.range(headerRow, ROWS_COUNT_FOR_HEADER)
                .boxed()
                .map(currentSheet::getRow)
                .collect(Collectors.toList());
        List<Map<Integer, String>> rowsValues = getRowsValues(rowsForProcess);
        List<Boolean> doesAnyContains = rowsValues.stream().map(row -> FunctionUtils.every(row.values(),
                s -> POSSIBLE_COMMENT_SIGNS.stream().anyMatch(s::startsWith))).collect(Collectors.toList());
        int skipRowsCount = 0;

        for (Boolean aBoolean : doesAnyContains) {
            if(aBoolean) {
                skipRowsCount++;
            } else {
                break;
            }
        }
        return skipRowsCount;
    }

    protected Map<Integer, String> getHeaders(Sheet myExcelSheet) {
        int firstRow = getFirstRowNum(myExcelSheet);
        return getRowsValues(asList(myExcelSheet.getRow(firstRow))).get(0);
    }

    private Integer defineHeader(List<Map<Integer, String>> rows) {
        OptionalInt possibleHeaderRow = IntStream.range(0, rows.size()).filter(rowIndex -> {
            Map<Integer, String> currentRow = rows.get(rowIndex);
            return currentRow.values().stream()
                    .map(DataType::tryGetType)
                    .filter(DataType.STRING::equals).collect(Collectors.toList()).size() == currentRow.size();
        }).findFirst();
        return possibleHeaderRow.orElse(0) + 1;

    }

    private List<Map<Integer, String>> getRowsValues(List<Row> rowsForProcess) {
        return rowsForProcess.stream().map(row -> {
            Map<Integer, String> headers = new LinkedHashMap<>();
            if(row != null) {
                long minColIx = row.getFirstCellNum();
                long maxColIx = row.getLastCellNum();
                String prefix = "Field ";
                for (long colIx = minColIx; colIx < maxColIx; colIx++) {
                    if(useHeaders) {
                        Cell cell = row.getCell((int) colIx);
                        if (cell == null || cell.getCellType() == Cell.CELL_TYPE_BLANK) {
                            continue;
                        }
                        if (cell.getCellType() == Cell.CELL_TYPE_FORMULA) {
                            evaluator.evaluateInCell(cell);
                        }
                        String name;
                        if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
                            name = cell.getStringCellValue();
                        } else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
                            name = String.valueOf(cell.getNumericCellValue());
                        } else if (cell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
                            name = String.valueOf(cell.getBooleanCellValue());
                        } else {
                            throw new RuntimeException("Can't define header name");
                        }
                        headers.put(cell.getColumnIndex(), name);
                    } else {
                        headers.put((int) colIx, prefix + colIx);
                    }
                }
            }
            return headers;
        }).collect(Collectors.toList());
    }

    public abstract Workbook getWorkbook(InputStream is) throws IOException;

    @Override
    public RecordIterator parse() throws IOException {
        int firstRow = Math.min(getFirstRowNum(currentSheet) + skipAfterHeader, 65535);

        Map<Integer, String> headers = getHeaders(currentSheet);
        return with(new RecordIterator() {

            int currentRow = useHeaders ? firstRow + 1 : firstRow;
            Map<AbstractParsedColumn, Object> currentObj;

            @Override
            public void close() throws IOException {}

            @Override
            public boolean hasNext() {
                return currentRow <= Math.max(currentSheet.getLastRowNum() - skipFromBottom, 0);
            }

            @Override
            public long getRowNumber() {
                return currentRow;
            }

            @Override
            public Map<AbstractParsedColumn, Object> getRaw() {
                return this.currentObj;
            }

            public Map<AbstractParsedColumn, Object> nextRaw(){
                if(!hasNext()){
                    return null;
                }
                Row row = currentSheet.getRow(++currentRow - 1);
                if (row == null) {
                    return new HashMap<>();
                }
                int minColIx = row.getFirstCellNum();
                int maxColIx = minColIx + headers.size();
                Map<AbstractParsedColumn, Object> o = new LinkedHashMap<>();
                for (int colIx = minColIx; colIx < maxColIx; colIx++) {
                    Cell cell = row.getCell(colIx);
                    String header = headers.get(colIx);
                    if (header == null) {
                        continue;
                    }
                    Object cellValue = cell == null ? null : getCellValue(cell);

                    if(cellValue != null) {
                        String cellValueStr = String.valueOf(cellValue);
                        cellValue = cellValueStr.equals("NULL") ? null : StringUtils.stripStart(cellValueStr, STRIP_CHARACTERS);
                    }

                    o.put(new IndexedParsedColumn(colIx, header), cellValue);
                }
                return o;
            }

            @Override
            public Map<AbstractParsedColumn, Object> next() {
                this.currentObj = postProcessWithNullBehaviour(nextRaw());
                return this.currentObj;
            }

            @Override
            public Map<AbstractParsedColumn, TypeDescriptor> getSchema() {
                Map<AbstractParsedColumn, Object> o = getRaw();
                if (o == null) {
                    return null;
                }
                Map<AbstractParsedColumn, TypeDescriptor> schema = new LinkedHashMap<>();
                for(Map.Entry<AbstractParsedColumn, Object> entry : o.entrySet()){
                    DataType dataType = DataType.tryGetType(entry.getValue());
                    if(dataType != null){
                        TypeDescriptor type;
                        if(dataType == DataType.STRING) {
                            type = DataType.tryParseType(Objects.toString(entry.getValue()));
                        } else if(dataType == DataType.DATE) {
                            type = DateTypeDescriptor.forValue((Date) entry.getValue());
                        } else if (dataType == DataType.TIME) {
                            type = new TimeTypeDescriptor();
                        } else if (dataType == DataType.DECIMAL) {
                            type = NumberTypeDescriptor.forValue((Double) entry.getValue());
                        } else {
                            type = new TypeDescriptor(dataType);
                        }
                        schema.put(entry.getKey(), type);
                    }
                }
                return schema;
            }

            @Override
            public long getBytesCount() {
                return -1; // not implemented
            }
        })
                .limited(descriptor.getLimit())
                .withTransforms(descriptor.getColumnTransforms())
                .withColumns(descriptor.getColumns())
                .interruptible()
                .build();
    }

    private Object getCellValue(Cell cell) {
        return getCellValue(cell, cell.getCellType());
    }

    private Object getCellValue(Cell cell, int cellType) {
        switch (cellType) {
            case Cell.CELL_TYPE_BOOLEAN:
                return cell.getBooleanCellValue();
            case Cell.CELL_TYPE_NUMERIC:
                double value = cell.getNumericCellValue();
                if (HSSFDateUtil.isCellDateFormatted(cell)) {
                    Date date = cell.getDateCellValue();
                    if(date.getTime() < 86_400_000){
                        date = new Time(date.getTime());
                    }
                    return date;
                } else {
                    return value;
                }
            case Cell.CELL_TYPE_STRING:
                return cell.getStringCellValue();
            case Cell.CELL_TYPE_ERROR:
                return FormulaError.forInt(cell.getErrorCellValue()).getString();
            // if formula has been evaluated incorrectly, just get string value
            case Cell.CELL_TYPE_FORMULA:
                try {
                    return getCellValue(cell, evaluator.evaluateFormulaCell(cell));
                } catch (Exception e){
                    log.debug("Can't evaluate formula at [" + (cell.getRowIndex() + 1) + "," + (cell.getColumnIndex() + 1) + "]: "
                            + ExceptionUtils.getRootCauseMessage(e));
                }
            default:
                return null;
        }
    }
}
