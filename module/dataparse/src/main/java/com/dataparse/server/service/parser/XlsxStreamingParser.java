package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.IndexedParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.XlsFileDescriptor;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.math.DoubleMath;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.*;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Time;
import java.util.*;

import static com.dataparse.server.service.parser.RecordIteratorBuilder.with;
import static org.apache.poi.ss.usermodel.DateUtil.getJavaDate;
import static org.apache.poi.ss.usermodel.DateUtil.isADateFormat;

@Slf4j
public class XlsxStreamingParser extends Parser {

    private static int getColumnIndexByName(String columnName) {
        int number = 0;
        for (int i = 0; i < columnName.length(); i++) {
            number = number * 26 + (columnName.charAt(i) - ('A' - 1));
        }
        return number - 1;
    }

    enum XssfDataType {
        BOOLEAN,
        ERROR,
        FORMULA,
        INLINE_STRING,
        SST_STRING,
        NUMBER,
    }

    private static final int VALID_HEADERS_DETECT_RETRIES = 5;
    private static final String STRIP_CHARACTERS = "'`";
    private static final Set<String> POSSIBLE_COMMENT_SIGNS = Sets.newHashSet("//");

    private XlsFileDescriptor descriptor;
    private FileStorage fileStorage;

    private File tmp;

    private boolean useHeaders;
    private int startOnRow;
    private int skipAfterHeader;
    private int skipFromBottom;

    public XlsxStreamingParser(final FileStorage fileStorage,
                               final XlsFileDescriptor descriptor) {
        this.descriptor = descriptor;
        this.fileStorage = fileStorage;

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
        tmp = copyRemoteFileIntoTmpFile(descriptor.getPath());
    }

    @Override
    public void close() throws IOException {
        super.close();
        tmp.delete();
    }

    private File copyRemoteFileIntoTmpFile(String path){
        try {
            InputStream is = fileStorage.getFile(path);
            File tmp = File.createTempFile(UUID.randomUUID().toString(), "");
            IOUtils.copyLarge(is, new FileOutputStream(tmp));
            return tmp;
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public RecordIterator parse() throws IOException {
        InputStream sheetIS;
        OPCPackage pkg;
        XSSFReader r;
        ReadOnlySharedStringsTable sst;
        StylesTable stylesTable;
        try {
            log.info("Started parsing XLSX sheet '{}'", descriptor.getSheetName());
            pkg = OPCPackage.open(tmp.getAbsolutePath(), PackageAccess.READ);
            r = new XSSFReader(pkg);
            sheetIS = r.getSheet(descriptor.getSheetName());
            sst = new ReadOnlySharedStringsTable(pkg);
            log.info("Parsed shared string table of {} entries", sst.getUniqueCount());
            stylesTable = r.getStylesTable();
            log.info("Parsed styles table");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DataFormatter formatter = new DataFormatter();
        XMLInputFactory factory = XMLInputFactory.newInstance();
        try {
            XMLEventReader eventReader = factory.createXMLEventReader(sheetIS);

            return with(new RecordIterator() {

                Map<AbstractParsedColumn, Object> o = null;
                Map<AbstractParsedColumn, Object> currentObj;

                boolean inV = false, sheetDataEnded = false;

                Map<AbstractParsedColumn, String> headerMap = null;

                boolean startedParsing = false;
                int lastRow = -1;
                String lastCell = null;
                String lastCellType = null;
                XssfDataType lastDataType = null;
                String lastContents = null;
                int formatIndex = -1;
                String formatString = null;

                private StartElement setCursorToNextRow(XMLEventReader eventReader) throws XMLStreamException {
                    XMLEvent event;
                    while(eventReader.hasNext()) {
                        event = eventReader.nextEvent();
                        switch (event.getEventType()) {
                            case XMLStreamConstants.START_ELEMENT:
                                StartElement startElement = (StartElement) event;
                                if(startElement.getName().getLocalPart().equals("row")){
                                    lastRow = Integer.parseInt(startElement.getAttributeByName(QName.valueOf("r")).getValue());
                                    return startElement;
                                }
                                break;
                            case XMLStreamConstants.END_ELEMENT:
                                EndElement endElement = (EndElement) event;
                                if (endElement.getName().getLocalPart().equals("sheetData")) {
                                    sheetDataEnded = true;
                                    return null;
                                }
                                break;
                        }
                    }
                    return null;
                }

                private String getColumn(String cellAddr){
                    return cellAddr.replaceAll("[\\d.]", "");
                }

                private String getColumnName(String cellAddr){
                    String column = getColumn(cellAddr);
                    if(useHeaders) {
                        String columnName = headerMap.get(column);
                        if(StringUtils.isNotBlank(columnName)){
                            return columnName;
                        }
                    }
                    return column;
                }

                private Map<AbstractParsedColumn, Object> getNextRow(XMLEventReader eventReader) throws XMLStreamException {
                    XMLEvent event;
                    Map<AbstractParsedColumn, Object> o = new LinkedHashMap<>();
                    while(eventReader.hasNext()){
                        event = eventReader.nextEvent();
                        switch (event.getEventType()) {
                            case XMLStreamConstants.START_ELEMENT:
                                StartElement childStartElement = (StartElement) event;
                                if(childStartElement.getName().getLocalPart().equals("c")) {
                                    lastCell = childStartElement.getAttributeByName(QName.valueOf("r")).getValue();
                                    Attribute lastCellTypeAttr = childStartElement.getAttributeByName(QName.valueOf("t"));
                                    lastCellType = lastCellTypeAttr == null ? null : lastCellTypeAttr.getValue();
                                    Attribute lastCellStyleAttr = childStartElement.getAttributeByName(QName.valueOf("s"));
                                    String cellStyleStr = lastCellStyleAttr == null ? null : lastCellStyleAttr.getValue();

                                    this.lastDataType = XssfDataType.NUMBER;
                                    this.formatIndex = -1;
                                    this.formatString = null;
                                    if ("b".equals(lastCellType))
                                        lastDataType = XssfDataType.BOOLEAN;
                                    else if ("e".equals(lastCellType))
                                        lastDataType = XssfDataType.ERROR;
                                    else if ("inlineStr".equals(lastCellType))
                                        lastDataType = XssfDataType.INLINE_STRING;
                                    else if ("s".equals(lastCellType))
                                        lastDataType = XssfDataType.SST_STRING;
                                    else if ("str".equals(lastCellType))
                                        lastDataType = XssfDataType.FORMULA;
                                    else {
                                        // Number, but almost certainly with a special style or format
                                        XSSFCellStyle style = null;
                                        if (stylesTable != null) {
                                            if (cellStyleStr != null) {
                                                int styleIndex = Integer.parseInt(cellStyleStr);
                                                style = stylesTable.getStyleAt(styleIndex);
                                            } else if (stylesTable.getNumCellStyles() > 0) {
                                                style = stylesTable.getStyleAt(0);
                                            }
                                        }
                                        if (style != null) {
                                            this.formatIndex = style.getDataFormat();
                                            this.formatString = style.getDataFormatString();
                                            if (this.formatString == null)
                                                this.formatString = BuiltinFormats.getBuiltinFormat(this.formatIndex);
                                        }
                                    }
                                }
                                if(childStartElement.getName().getLocalPart().equals("v")){
                                    inV = true;
                                }
                                lastContents = "";
                                break;
                            case XMLStreamConstants.CHARACTERS:
                                Characters characters = (Characters) event;
                                if(inV) {
                                    lastContents += characters.getData();
                                }
                                break;
                            case XMLStreamConstants.END_ELEMENT:
                                EndElement endElement = (EndElement) event;
                                if(endElement.getName().getLocalPart().equals("v")){
                                    inV = false;
                                    Object v;

                                    switch (this.lastDataType){
                                        case BOOLEAN:
                                            char first = lastContents.charAt(0);
                                            v = first != '0';
                                            break;

                                        case ERROR:
                                            v = "ERROR:" + lastContents;
                                            break;

                                        case FORMULA:

                                            String fv = lastContents;

                                            if (this.formatString != null) {
                                                try {
                                                    // Try to use the value as a formattable number
                                                    double d = Double.parseDouble(fv);
                                                    v = formatter.formatRawCellContents(d, this.formatIndex, this.formatString);
                                                } catch(NumberFormatException e) {
                                                    // Formula is a String result not a Numeric one
                                                    v = fv;
                                                }
                                            } else {
                                                // No formating applied, just do raw value in all cases
                                                v = fv;
                                            }

                                            break;

                                        case INLINE_STRING:
                                            // TODO: Can these ever have formatting on them?
                                            XSSFRichTextString rtsi = new XSSFRichTextString(lastContents);
                                            v = rtsi.toString();
                                            break;

                                        case SST_STRING:
                                            String sstIndex = lastContents;
                                            try {
                                                int idx = Integer.parseInt(sstIndex);
                                                v = sst.getEntryAt(idx);
                                            }
                                            catch (NumberFormatException ex) {
                                                log.error("Failed to parse SST index '" + sstIndex, ex);
                                                v = null;
                                            }
                                            break;

                                        case NUMBER:
                                            String n = lastContents;

                                            if (this.formatString != null && n.length() > 0) {
                                                double value = Double.parseDouble(n);
                                                if (isCellDateFormatted(value, this.formatIndex, this.formatString)) {
                                                    Date date = getJavaDate(value, TimeZone.getTimeZone("UTC"));
                                                    if(date.getTime() < 86_400_000){
                                                        date = new Time(date.getTime());
                                                    }
                                                    v = date;
                                                } else {
                                                    if (DoubleMath.isMathematicalInteger(value)) {
                                                        v = (long) value;
                                                    } else {
                                                        v = value;
                                                    }
                                                }
                                            } else {
                                                v = n;
                                            }
                                            break;

                                        default:
                                            throw new RuntimeException("Unexpected type: " + lastDataType);
                                    }
                                    String column = getColumn(lastCell);
                                    o.put(new IndexedParsedColumn(getColumnIndexByName(column) , column), v);
                                }
                                if(endElement.getName().getLocalPart().equals("row")) {
                                    return presetLeftItems(o);
                                }
                        }
                    }
                    return presetLeftItems(o);
                }

                private Map<AbstractParsedColumn, Object> presetLeftItems(Map<AbstractParsedColumn, Object> row) {
                    Map<AbstractParsedColumn, Object> result = new LinkedHashMap<>();
                    if(this.headerMap != null) {
                        this.headerMap.keySet().forEach(key -> result.put(key, row.get(key)));
                        return result;
                    } else {
                        return row;
                    }
                }

                private Map<AbstractParsedColumn, String> getHeaders(XMLEventReader eventReader) throws XMLStreamException {
                    Map<AbstractParsedColumn, Object> firstRow = getNextRow(eventReader);

                    Integer inspectedRows = 0;

                    if(getNullableHeadersFromRow(firstRow) == null) {
                        while (inspectedRows < VALID_HEADERS_DETECT_RETRIES) {
                            Map<AbstractParsedColumn, Object> nextRow = getNextRow(eventReader);
                            Map<AbstractParsedColumn, String> headers = getNullableHeadersFromRow(nextRow);
                            if (headers != null) {
                                return headers;
                            }

                            inspectedRows++;
                        }
                    }

                    return getHeadersFromRow(firstRow);
                }

                private boolean rowHasComments(Map<AbstractParsedColumn, Object> row) {
                    for(Object cell : row.values()) {
                        if(cell == null) {
                            continue;
                        }

                        boolean has = POSSIBLE_COMMENT_SIGNS.stream().anyMatch(sign -> cell.toString().startsWith(sign));
                        if(has) {
                            return true;
                        }
                    }

                    return false;
                }

                private Map<AbstractParsedColumn, String> getHeadersFromRow(Map<AbstractParsedColumn, Object> row) {
                    Map<AbstractParsedColumn, String> headers = new HashMap<>();
                    for(Map.Entry<AbstractParsedColumn, Object> entry : row.entrySet()) {
                        String columnName = String.valueOf(entry.getValue());
                        AbstractParsedColumn column = entry.getKey();

                        column.setColumnName(columnName);
                        headers.put(column, columnName);
                    }
                    return headers;
                }

                private Map<AbstractParsedColumn, String> getNullableHeadersFromRow(Map<AbstractParsedColumn, Object> row) {
                    int totalHeadersCount = row.entrySet().size();
                    int uniqueCount = new HashSet<>(row.values()).size();

                    if(totalHeadersCount != uniqueCount) {
                        return null;
                    } else {
                        return getHeadersFromRow(row);
                    }
                }

                private Map<AbstractParsedColumn, Object> tryParseNext(XMLEventReader eventReader) throws XMLStreamException {
                    StartElement row = setCursorToNextRow(eventReader);

                    int skipped = 0;
                    Map<AbstractParsedColumn, Object> o = null;

                    if(!startedParsing) {
                        startedParsing = true;
                        while(startOnRow > lastRow && row != null) {
                            row = setCursorToNextRow(eventReader);
                        }
                        if(row == null) {
                            return null;
                        }
                        if(useHeaders) {
                            headerMap = getHeaders(eventReader);
                            row = setCursorToNextRow(eventReader);
                        }

                        while(skipAfterHeader > skipped ++) {
                            row = setCursorToNextRow(eventReader);
                        }

                        // check commented
                        while(row != null) {
                            o = getNextRow(eventReader);

                            if(o != null && rowHasComments(o)) {
                                skipped ++;
                            } else {
                                break;
                            }
                        }
                    }

                    // temporary force comments trim [unable to locate them from the outside]
                    skipAfterHeader = skipped;
                    descriptor.getSettings().setSkipAfterHeader(skipped);

                    if(row == null) {
                        return null;
                    }

                    if(o == null) {
                        o = getNextRow(eventReader);
                    }

                    Map<AbstractParsedColumn, Object> result = new LinkedHashMap<>();
                    for(Map.Entry<AbstractParsedColumn, Object> entry : o.entrySet()){
                        Object value = entry.getValue();
                        if(value != null) {
                            String valueStr = String.valueOf(value);
                            value = valueStr.equals("NULL") ? null : StringUtils.stripStart(valueStr, STRIP_CHARACTERS);
                        }

                        result.put(entry.getKey(), value);
                    }
                    return result;
                }

                @Override
                public Map<AbstractParsedColumn, Object> getRaw() {
                    return this.currentObj;
                }

                @Override
                public long getBytesCount() {
                    return -1;
                }

                @Override
                public void close() throws IOException {
                    try {
                        eventReader.close();
                        sheetIS.close();
                        pkg.close();
                    } catch (XMLStreamException e) {
                        throw new IOException(e);
                    }
                }

                @Override
                public boolean hasNext() {
                    if(o == null){
                        try {
                            o = tryParseNext(eventReader);
                        } catch (XMLStreamException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return !sheetDataEnded && o != null;
                }

                public Map<AbstractParsedColumn, Object> nextRaw() {
                    if(o != null) {
                        Map<AbstractParsedColumn, Object> tmp = o;
                        o = null;
                        return tmp;
                    } else {
                        try {
                            return tryParseNext(eventReader);
                        } catch (XMLStreamException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                @Override
                public Map<AbstractParsedColumn, Object> next() {
                    this.currentObj = postProcessWithNullBehaviour(nextRaw());
                    return this.currentObj;
                }
            })
                    .limited(descriptor.getLimit())
                    .withTransforms(descriptor.getColumnTransforms())
                    .withColumns(descriptor.getColumns())
                    .withSkippedRows(0, skipFromBottom)
                    .interruptible()
                    .build();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    private static boolean isCellDateFormatted(Double d, int i, String f) {
        if (d == null) return false;
        boolean bDate = false;

        if ( DateUtil.isValidExcelDate(d) ) {
            bDate = isADateFormat(i, f);
        }
        return bDate;
    }

    @Override
    public Pair<Long, Boolean> getRowsEstimateCount(final long fileSize) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try(OPCPackage pkg = OPCPackage.open(tmp.getAbsolutePath(), PackageAccess.READ);
            InputStream sheetIS = new XSSFReader(pkg).getSheet(descriptor.getSheetName())) {

            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLEventReader eventReader = factory.createXMLEventReader(sheetIS);
            XMLEvent event;
            while(eventReader.hasNext()) {
                event = eventReader.nextEvent();
                switch (event.getEventType()) {
                    case XMLStreamConstants.START_ELEMENT:
                        StartElement startElement = (StartElement) event;
                        if(startElement.getName().getLocalPart().equals("dimension")){
                            String dimension = startElement.getAttributeByName(QName.valueOf("ref")).getValue();
                            String[] bbox = dimension.split(":");
                            if(bbox.length < 2){
                                return null;
                            }
                            int startRow = Math.max(startOnRow, new CellAddress(bbox[0]).getRow() + 1);
                            int endRow = new CellAddress(bbox[1]).getRow() + 1;
                            long range = endRow - startRow + 1L;
                            if(useHeaders){
                                range--;
                            }
                            if(skipAfterHeader > 0){
                                range -= skipAfterHeader;
                            }
                            if(skipFromBottom > 0){
                                range -= skipFromBottom;
                            }
                            if(range < 0){
                                range = 0;
                            }
                            return Pair.of(range, true);
                        } else if(startElement.getName().getLocalPart().equals("sheetData")){
                            return null;
                        }
                        break;
                }
            }
        } catch (Exception e){
            log.error("Can't get row size estimate", e);
        } finally {
            log.info("Define rows estimate count took: {}", stopwatch.stop());
        }
        return null;
    }

    public Map<String, String> getSheets() {
        try {
            OPCPackage pkg = OPCPackage.open(tmp.getAbsolutePath(), PackageAccess.READ);
            XSSFReader r = new XSSFReader(pkg);
            InputStream w = r.getWorkbookData();
            Map<String, String> sheets = new LinkedHashMap<>();
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(false);
            factory.setValidating(false);
            factory.setFeature("http://xml.org/sax/features/namespaces", false);
            factory.setFeature("http://xml.org/sax/features/validation", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(w);
            XPathFactory xPathfactory = XPathFactory.newInstance();
            XPath xpath = xPathfactory.newXPath();
            XPathExpression expr = xpath.compile("//workbook/sheets/*");
            NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
            for(int i = 0; i < nodes.getLength(); i++){
                NamedNodeMap attrs = nodes.item(i).getAttributes();
                sheets.put(attrs.getNamedItem("r:id").getNodeValue(), attrs.getNamedItem("name").getNodeValue());
            }
            return sheets;
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

}
