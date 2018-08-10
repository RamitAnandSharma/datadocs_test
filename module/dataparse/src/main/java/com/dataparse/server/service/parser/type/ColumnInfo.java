package com.dataparse.server.service.parser.type;

import com.dataparse.server.service.flow.ErrorValue;
import com.dataparse.server.service.flow.settings.ColumnSettings;
import com.dataparse.server.service.flow.settings.SearchType;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.visualization.bookmark_state.state.ColFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.persistence.*;
import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@Slf4j
@Entity
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@EqualsAndHashCode(exclude = { "id", "alias", "pkey", "disableFacets", "exampleValue",
        "settings", "type", "errorCount", "firstError", "firstErrorDescription"})
public class ColumnInfo implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    private String name;
    private String alias;
    private boolean pkey;
//    may be null, for some type of datasources (JSON, XML, etc..)
    private Integer initialIndex;
    private Boolean disableFacets;
    private boolean repeated;
    private String exampleValue;

    public Integer getInitialIndex() {
        return this.getSettings().getInitialIndex() != null ? this.getSettings().getInitialIndex() : this.initialIndex;
    }

    public boolean isRepeated() {
        return (this.getSettings() != null && this.getSettings().getSplitOn() != null) || this.repeated;
    }

    public void setInitialIndex(Integer initialIndex) {
        this.initialIndex = initialIndex;
        this.getSettings().setInitialIndex(initialIndex);
    }

    public void setName(String name) {
        this.name = cutTheString(name);
    }

    public String getName() {
        if(this.name != null) {
            return cutTheString(name);
        }
        return null;
    }

    public void setExampleValue(String exampleValue) {
        if(exampleValue != null) {
            this.exampleValue = cutTheString(exampleValue);
        }
    }

    private String cutTheString(String value) {
        if(value.length() < 255) {
            return value;
        } else {
            return value.substring(0, 254);
        }
    }

    public ColumnInfo(final String name, final TypeDescriptor type) {
        this.name = name;
        this.type = type;
    }

    @Transient
    public String getKey(){
        String rename = settings.getRename();
        return StringUtils.isBlank(rename) ? name : rename;
    }

    @Transient
    public String getColumnInfoIdentifier(){
        return ParsedColumnFactory.getByColumnInfo(this).toString();
    }

    @Transient
    private ColFormat.Type formatType;

    @Transient
    ColumnSettings settings = new ColumnSettings();

    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private TypeDescriptor type;

    public static List<ColumnInfo> aggregateColumns(List<Descriptor> descriptors){
        return aggregateColumns(descriptors, false);
    }

    public Boolean isDateColumn() {
        if(this.type == null) {
            return false;
        }
        return Sets.newHashSet(DataType.DATE, DataType.TIME).contains(this.type.getDataType());
    }

    public Boolean isRemoved() {
        return this.settings.isRemoved();
    }

    public int compareTo(ColumnInfo c2) {
        if (this.getSettings().getIndex() != null && c2.getSettings().getIndex() != null) {
            return this.getSettings().getIndex().compareTo(c2.getSettings().getIndex());
        } else if (this.getSettings().getIndex() != null) {
            return 1;
        } else if (c2.getSettings().getIndex() != null) {
            return -1;
        }
        return 0;
    }

    public static List<ColumnInfo> aggregateColumns(List<Descriptor> descriptors, boolean joinColumns){
        Multimap<Pair<Integer, String>, ColumnInfo> groupedByNameColumns = LinkedHashMultimap.create();

        int additionalIndex = 0;
        boolean noColumnInfo = true;
        for(Descriptor descriptor : descriptors){
            if(descriptor.getColumns() != null) {
                noColumnInfo = false;
                for (ColumnInfo columnInfo : descriptor.getColumns()) {
                    if (!columnInfo.getSettings().isRemoved()) {
                        String name = columnInfo.getSettings().getName() != null ? columnInfo.getSettings().getName() : columnInfo.getName();
                        if (StringUtils.isNotBlank(columnInfo.getSettings().getRename())) {
                            name = columnInfo.getSettings().getRename();
                        }
                        if (joinColumns && columnInfo.getSettings().getIndex() != null) {
                            columnInfo.getSettings().setIndex(columnInfo.getSettings().getIndex() + additionalIndex);
                        }
                        groupedByNameColumns.put(Pair.of(columnInfo.getSettings().getIndex(), name), columnInfo);
                    }
                }
                additionalIndex += descriptor.getColumns().size();
            }
        }
        if(noColumnInfo){
            return null;
        }
        List<ColumnInfo> resultColumns = new ArrayList<>();
        boolean pkFound = false;
        for(Pair<Integer, String> nameAndIndex : groupedByNameColumns.keySet()){
            String name = nameAndIndex.getRight();
            Collection<ColumnInfo> columns = groupedByNameColumns.get(nameAndIndex);
            ColumnInfo column = new ColumnInfo();
            column.setName(name);
            column.getSettings().setName(columns.stream().map(c -> Optional.ofNullable(c.getSettings().getName()))
                    .findFirst().flatMap(Function.identity()).orElse(name));
            column.setAlias(columns.stream().map(c -> Optional.ofNullable(c.getAlias()))
                    .findFirst().flatMap(Function.identity()).orElse(null));
            column.getSettings().setAnnotation(columns.stream().map(c -> c.getSettings().getAnnotation())
                    .filter(a -> a != null).reduce((a1, a2) -> a1 + "\n\n" + a2).orElse(null));
            column.getSettings().setIndex(columns.stream().map(c -> Optional.ofNullable(c.getSettings().getIndex()))
                    .findFirst().flatMap(Function.identity()).orElse(null));
            column.getSettings().setDisableFacets(columns.stream().map(c -> c.getSettings().isDisableFacets()).anyMatch(c -> c));
            column.getSettings().setFormatType(columns.stream().map(c -> Optional.ofNullable(c.getSettings().getFormatType()))
                    .findFirst().flatMap(Function.identity()).orElse(null));
            column.setType(TypeDescriptor.getCommonType(columns.stream().map(c -> c.getSettings().getType() == null ? c.getType() : c.getSettings().getType()).collect(Collectors.toList())));
            column.setRepeated(columns.iterator().next().isRepeated() || columns.iterator().next().getSettings().getSplitOn() != null);
            column.getSettings().setType(column.getType());
            column.getSettings().setSearchType(columns.stream().map(c -> Optional.ofNullable(c.getSettings().getSearchType())).min(
                    Comparator.comparing(Optional::get)).get().orElse(SearchType.NONE));
            column.setExampleValue(columns.stream().map(c -> Optional.ofNullable(getExampleValueFormatted(column, c.getExampleValue())))
                    .findFirst().flatMap(Function.identity()).orElse(null));
            //  allow single pk only todo do not set pkey on columninfo, just on settings!
            if(!pkFound) {
                column.getSettings().setPkey(columns.stream().map(c -> Optional.ofNullable(c.getSettings().isPkey()))
                        .reduce((x, y) -> Optional.of(x.get() || y.get())).flatMap(Function.identity()).orElse(false));
                column.setPkey(column.getSettings().isPkey());
                if(column.getSettings().isPkey()) {
                    pkFound = true;
                }
            } else {
                column.getSettings().setPkey(false);
                column.setPkey(false);
            }
            resultColumns.add(column);
        }
        return resultColumns;
    }

    public static String getExampleValueFormatted(ColumnInfo column, String exampleValue) {
        try {
            if(exampleValue == null) {
                return null;
            }
            TypeDescriptor typeDescriptor = column.getSettings().getType();
            if(column.isRepeated()) {
                return exampleValue;
            } else if(typeDescriptor instanceof NumberTypeDescriptor) {
                NumberTypeDescriptor numberTypeDescriptor = (NumberTypeDescriptor) typeDescriptor;
                if(numberTypeDescriptor.isInteger()) {
                    return String.valueOf(Double.valueOf(exampleValue).longValue());
                } else {
                    return String.format("%.2f", Double.parseDouble(exampleValue));
                }
            } else if (typeDescriptor instanceof DateTypeDescriptor) {
                return exampleValue;
            }
            return exampleValue;
        } catch (Exception e) {
            log.info("Can not retrieve example value '{}'", exampleValue, e);
        }
        return null;
    }

    @Transient
    private int errorCount;

    @Transient
    private ErrorValue firstError;

    @Deprecated
    @Transient
    private String firstErrorDescription;

}
