package com.dataparse.server.service.visualization.bookmark_state.state;

import com.dataparse.server.service.flow.settings.SearchType;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.parser.type.DateTypeDescriptor;
import com.dataparse.server.service.parser.type.NumberTypeDescriptor;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.text.WordUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class Col implements Serializable {

    public Col(String field, String originalField, String name, DataType type, String exampleValue){
        this(field, originalField, name);
        this.type = type;
        this.exampleValue = exampleValue;
    }

    public Col(String field, String originalField, String name, DataType type){
        this(field, originalField, name);
        this.type = type;
    }

    public Col(String field, String originalField, String name){
        this.field = field;
        this.originalField = originalField;
        this.name = name;
        this.settings = new ColSettings();
    }

    public Col(String field, String name, DataType type) {
        this(field, field, name, type, null);
    }

    public Col(String field, String name){
        this(field, field, name);
    }

    /*col name in database*/
    private String field;
    /*col name that come from source*/
    private String originalField;
    /*name that we will show in UI*/
    private String name;
    private DataType type;
    private AbstractParsedColumn initialColumnInfo;
    private boolean repeated;
    private SearchType searchType;
    private boolean disableFacets;
    private ColSettings settings;
    private String exampleValue;

    private static String toName(String field){
        String prepared = field
                .replace("\\.", ".")
                .replace("_", " ")
                .toLowerCase()
                .replaceAll("\\bid\\b", "ID");
        return WordUtils.capitalize(prepared, '.', '-', '(', ')');
    }

    public void updateFrom(ColumnInfo column){
        this.setDisableFacets(Optional.ofNullable(column.getDisableFacets()).orElse(false));
        this.getSettings().setSearchType(column.getSettings().getSearchType());
        this.setRepeated(column.isRepeated());
        this.setSearchType(column.getSettings().getSearchType());
        this.exampleValue = column.getExampleValue();
        this.getSettings().setFormat(new ColFormat());
        this.initialColumnInfo = ParsedColumnFactory.getByColumnInfo(column);
        if(column.getSettings() != null && column.getSettings().getFormatType() != null){
            this.getSettings().getFormat().setType(column.getSettings().getFormatType());
        } else {
            switch (column.getType().getDataType()) {
                case DATE:
                    if(column.getType() instanceof DateTypeDescriptor) {
                        DateTypeDescriptor dateTypeDescriptor = (DateTypeDescriptor) column.getType();
                        ColFormat.Type dateFormat;
                        if(dateTypeDescriptor.isNoTime()){
                            dateFormat = ColFormat.Type.DATE_1;
                        } else {
                            dateFormat = ColFormat.Type.DATE_TIME;
                        }
                        this.getSettings().getFormat().setType(dateFormat);
                    }
                    break;
                case TIME:
                    this.getSettings().getFormat().setType(ColFormat.Type.TIME);
                    break;
                case DECIMAL:
                    this.getSettings().getFormat().setType(ColFormat.Type.NUMBER);
                    NumberTypeDescriptor numberTypeDescriptor = (NumberTypeDescriptor) column.getType();
                    this.getSettings().getFormat().setDecimalPlaces(numberTypeDescriptor.isInteger() ? 0 : 2);
                    this.getSettings().getFormat().setPossibleMillisTimestamp(numberTypeDescriptor.isPossibleMillisTimestamp());
                    this.getSettings().getFormat().setDefaultDecimalPlaces(this.getSettings().getFormat().getDecimalPlaces());
                    break;
                default:
                    this.getSettings().getFormat().setType(ColFormat.Type.TEXT);
                    break;
            }
        }
    }

    public static Col from(ColumnInfo column){
        Col col = new Col(column.getAlias(), column.getSettings().getName(), column.getName(),
                column.getType().getDataType(), column.getExampleValue());
        col.updateFrom(column);
        return col;
    }

    public static List<Col> from(List<ColumnInfo> columns){
        return columns.stream().map(Col::from).collect(Collectors.toList());
    }

}
