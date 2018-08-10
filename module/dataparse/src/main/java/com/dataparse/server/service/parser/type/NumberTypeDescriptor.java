package com.dataparse.server.service.parser.type;

import com.google.common.math.*;
import lombok.*;

import javax.persistence.*;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@DiscriminatorValue("n")
public class NumberTypeDescriptor extends TypeDescriptor {

    private static final long UNIX_TIMESTAMP_THRESHOLD = System.currentTimeMillis() / 1000 * 2;

    private boolean integer;
    private boolean possibleMillisTimestamp;

    @Override
    public DataType getDataType() {
        return DataType.DECIMAL;
    }

    public static NumberTypeDescriptor forValue(Double v){
        return new NumberTypeDescriptor(DoubleMath.isMathematicalInteger(v),
                                        v > UNIX_TIMESTAMP_THRESHOLD);
    }

}
