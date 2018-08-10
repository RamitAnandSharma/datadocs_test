package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ColumnsUtils;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.XmlFileDescriptor;
import com.dataparse.server.util.FlattenXml;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.CountingInputStream;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.*;
import java.io.*;
import java.util.*;

import static com.dataparse.server.service.parser.RecordIteratorBuilder.with;

@Slf4j
public class XmlParser extends Parser {

    public static final long AVAILABLE_PATHS_PARSING_LIMIT_BYTE = 10 * 1024 * 1024; //10MB

    private FileStorage fileStorage;
    private XmlFileDescriptor descriptor;

    public XmlParser(FileStorage fileStorage, XmlFileDescriptor descriptor) {
        this.fileStorage = fileStorage;
        this.descriptor = descriptor;
    }

    private static String getRowXPath(List<String> pathSegments){
        return "//" + String.join("/", pathSegments);
    }

    private static List<String> getPathSegments(String xpath) {
        if(!xpath.startsWith("//")){
            throw new RuntimeException("Invalid XPath expression");
        }
        return Arrays.asList(xpath.substring(2).split("/"));
    }

    public static Multiset<String> getAvailableRowXPaths(InputStream is){
        Multiset<String> availableRowXPaths = HashMultiset.create();
        CountingInputStream cis = new CountingInputStream(is);
        Reader reader = new InputStreamReader(cis);
        XMLInputFactory factory = XMLInputFactory.newInstance();
        try {
            XMLEventReader eventReader = factory.createXMLEventReader(reader);
            XMLEvent event;
            LinkedList<String> currentPathSegments = new LinkedList<>();
            while(eventReader.hasNext() && cis.getByteCount() < AVAILABLE_PATHS_PARSING_LIMIT_BYTE) {
                event = eventReader.nextEvent();
                switch (event.getEventType()) {
                    case XMLStreamConstants.START_ELEMENT:
                        StartElement startElement = (StartElement) event;
                        currentPathSegments.add(startElement.getName().getLocalPart());
                        String rowXPath = getRowXPath(currentPathSegments);
                        availableRowXPaths.add(rowXPath);
                        break;
                    case XMLStreamConstants.END_ELEMENT:
                        currentPathSegments.removeLast();
                        break;
                }
            }

        } catch (Exception e){
            throw new RuntimeException(e);
        }
        return availableRowXPaths;
    }

    @Override
    public RecordIterator parse() throws IOException {
        InputStream is = fileStorage.getFile(descriptor.getPath());
        CountingInputStream cis = new CountingInputStream(is);

        Reader reader = new InputStreamReader(cis);
        XMLInputFactory factory = XMLInputFactory.newInstance();
        try {
            XMLEventReader eventReader = factory.createXMLEventReader(reader);

            return with(new RecordIterator() {

                StartElement currentStartElement = null;
                StartElement firstRowElement = null;
                Map<AbstractParsedColumn, Object> o = null;
                Map<AbstractParsedColumn, Object> currentObj;

                private Map<String, Object> tryParseTag(StartElement startElement, XMLEventReader eventReader) throws XMLStreamException {
                    XMLEvent event;
                    Map<String, Object> o = new LinkedHashMap<>();

                    startElement.getAttributes().forEachRemaining(x -> {
                        Attribute attr = (Attribute) x;
                        o.put("[" + attr.getName().getLocalPart() + "]", attr.getValue());
                    });

                    while(eventReader.hasNext()){
                        event = eventReader.nextEvent();
                        switch (event.getEventType()) {
                            case XMLStreamConstants.START_ELEMENT:
                                StartElement childStartElement = (StartElement) event;
                                Object nestedValue = o.get(childStartElement.getName().getLocalPart());
                                if(nestedValue != null){
                                    Map<String, Object> v = tryParseTag(childStartElement, eventReader);
                                    List l;
                                    if(nestedValue instanceof List){
                                        l = (List) nestedValue;
                                    } else {
                                        l = new ArrayList();
                                        l.add(nestedValue);
                                        o.put(childStartElement.getName().getLocalPart(), l);
                                    }
                                    l.add(v);
                                } else {
                                    o.put(childStartElement.getName().getLocalPart(), tryParseTag(childStartElement, eventReader));
                                }

                                break;
                            case XMLStreamConstants.ATTRIBUTE:
                                Attribute attribute = (Attribute) event;
                                o.put("[" + attribute.getName().getLocalPart() + "]", attribute.getValue());
                                break;
                            case XMLStreamConstants.CHARACTERS:
                                Characters characters = (Characters) event;
                                if(!characters.isWhiteSpace()) {
                                    o.put("@content", characters.getData());
                                }
                                break;
                            case XMLStreamConstants.END_ELEMENT:

                                EndElement endElement = (EndElement) event;
                                if(endElement.getName().getLocalPart().equals(startElement.getName().getLocalPart())) {
                                    return o;
                                } else {
                                    log.error("For open tag {} found closing tag {}",
                                            startElement.getName().getLocalPart(),
                                            endElement.getName().getLocalPart());
                                    throw new RuntimeException("XML is not valid");
                                }
                        }
                    }
                    return o;
                }

                private StartElement getFirstRow(String rowXPath, XMLEventReader eventReader, boolean a) throws XMLStreamException {
                    List<String> pathSegments = getPathSegments(rowXPath);
                    XMLEvent event;
                    LinkedList<String> currentPathSegments = new LinkedList<>();
                    if(a){
                        currentPathSegments.addAll(pathSegments);
                        currentPathSegments.removeLast();
                    }
                    while(eventReader.hasNext()) {
                        event = eventReader.nextEvent();
                        switch (event.getEventType()) {
                            case XMLStreamConstants.START_ELEMENT:
                                StartElement startElement = (StartElement) event;
                                currentPathSegments.add(startElement.getName().getLocalPart());
                                if(pathSegments.equals(currentPathSegments)){
                                    return startElement;
                                }
                                break;
                            case XMLStreamConstants.END_ELEMENT:
                                currentPathSegments.removeLast();
                                break;
                        }
                    }
                    return null;
                }

                private Map<AbstractParsedColumn, Object> tryParseNext(String rowXPath, XMLEventReader eventReader) throws XMLStreamException {
                    Map<String, Object> o = null;
                    if(currentStartElement == null) {
                        currentStartElement = getFirstRow(rowXPath, eventReader, false);
                        firstRowElement = currentStartElement;
                    } else {
                        currentStartElement = getFirstRow(rowXPath, eventReader, true);
                    }
                    if(currentStartElement != null){
                        o = new LinkedHashMap<>();
                        o.put(currentStartElement.getName().getLocalPart(), tryParseTag(currentStartElement, eventReader));
                    }
                    return ColumnsUtils.namedColumnsFromMap(FlattenXml.flatten(o));
                }

                @Override
                public void close() throws IOException {
                    try {
                        eventReader.close();
                    } catch (XMLStreamException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public boolean hasNext() {
                    if(o == null){
                        try {
                            o = tryParseNext(descriptor.getSettings().getRowXPath(), eventReader);
                        } catch (XMLStreamException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return o != null;
                }

                @Override
                public Map<AbstractParsedColumn, Object> getRaw() {
                    return this.currentObj;
                }

                public Map<AbstractParsedColumn, Object> nextRaw() {
                    if(o != null){
                        Map<AbstractParsedColumn, Object> tmp = o;
                        o = null;
                        return tmp;
                    } else {
                        try {
                            return tryParseNext(descriptor.getSettings().getRowXPath(), eventReader);
                        } catch (XMLStreamException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                @Override
                public Map<AbstractParsedColumn, Object> next(){
                    this.currentObj = nextRaw();
                    return this.currentObj;
                }

                @Override
                public long getBytesCount() {
                    return cis.getByteCount();
                }
            })
                    .limited(descriptor.getLimit())
                    .withTransforms(descriptor.getColumnTransforms())
                    .withColumns(descriptor.getColumns())
                    .interruptible()
                    .build();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

}
