package com.dataparse.server.service.parser.type;


import com.dataparse.server.util.hibernate.*;

public class DataTypeUserType extends PersistentEnumUserType<DataType> {

    @Override
    public Class<DataType> returnedClass() {
        return DataType.class;
    }
}
