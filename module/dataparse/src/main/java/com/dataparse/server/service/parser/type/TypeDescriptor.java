package com.dataparse.server.service.parser.type;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.*;

import javax.persistence.*;
import javax.persistence.Entity;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

import static com.dataparse.server.service.parser.type.DataType.*;

@Data
@Entity
@EqualsAndHashCode(exclude = {"id"})
@Inheritance(strategy=InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(
        name="discr",
        discriminatorType=DiscriminatorType.CHAR
)
@DiscriminatorValue("c")
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class TypeDescriptor implements Serializable {

    @Id
    @Column(name="id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Type(type="com.dataparse.server.service.parser.type.DataTypeUserType")
    private DataType dataType;

    public TypeDescriptor(DataType dataType) {
        this.dataType = dataType;
    }

    public Object parse(String s) throws Exception {
        return getDataType().parse(s);
    }

    public static TypeDescriptor getCommonType(List<TypeDescriptor> tds) {
        if(tds.isEmpty()){
            return null;
        } else if(tds.size() == 1){
            return tds.get(0);
        } else {
            TypeDescriptor descr = tds.get(0);
            for(int i = 1; i < tds.size(); i++){
                TypeDescriptor currentDescr = tds.get(i);
                descr = getCommonType(descr, currentDescr);
            }
            return descr;
        }
    }

    private static final Set<DataType> DECIMALS = Sets.newHashSet(DECIMAL);
    private static final Set<DataType> DATES = Sets.newHashSet(DATE);

    public static TypeDescriptor getCommonType(TypeDescriptor t1, TypeDescriptor t2) {
        if(t1 == t2) {
            return t1;
        } else if(t1 == null) {
            return t2;
        } else if (t2 == null) {
            return t1;
        } else {
            if(t1.equals(t2)) {
                return t1;
            }
            Set<DataType> types = Sets.newHashSet(t1.getDataType(), t2.getDataType());
            if (types.equals(DECIMALS)) {
                boolean integer = ((NumberTypeDescriptor) t1).isInteger() && ((NumberTypeDescriptor) t2).isInteger();
                boolean possibleTimestampMillis = ((NumberTypeDescriptor) t1).isPossibleMillisTimestamp()
                        || ((NumberTypeDescriptor) t2).isPossibleMillisTimestamp();
                return new NumberTypeDescriptor(integer, possibleTimestampMillis);
            } else {
                return new TypeDescriptor(STRING);
            }
        }
    }
}
