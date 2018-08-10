package com.dataparse.server.util.hibernate;

import lombok.extern.slf4j.*;
import org.hibernate.*;
import org.hibernate.engine.spi.*;
import org.hibernate.usertype.*;

import java.io.*;
import java.sql.*;

@Slf4j
public abstract class PersistentEnumUserType<T extends PersistentEnum> implements UserType {

    @Override
    public Object assemble(Serializable cached, Object owner)
            throws HibernateException {
        return cached;
    }

    @Override
    public Object deepCopy(Object value) throws HibernateException {
        return value;
    }

    @Override
    public Serializable disassemble(Object value) throws HibernateException {
        return (Serializable)value;
    }

    @Override
    public boolean equals(Object x, Object y) throws HibernateException {
        return x == y;
    }

    @Override
    public int hashCode(Object x) throws HibernateException {
        return x == null ? 0 : x.hashCode();
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public Object nullSafeGet(ResultSet rs, String[] names, SharedSessionContractImplementor session, Object owner)
            throws HibernateException, SQLException {
        int id = rs.getInt(names[0]);
        if(rs.wasNull()) {
            return null;
        }
        for(PersistentEnum value : returnedClass().getEnumConstants()) {
            if(id == value.getId()) {
                return value;
            }
        }
//        log.info("{}", id);
//        return null;
        throw new IllegalStateException("Unknown " + returnedClass().getSimpleName() + " id");
    }

    @Override
    public void nullSafeSet(PreparedStatement st, Object value, int index, SharedSessionContractImplementor session)
            throws HibernateException, SQLException {
        if (value == null) {
            st.setNull(index, Types.INTEGER);
        } else {
            st.setInt(index, ((PersistentEnum)value).getId());
        }
    }

    @Override
    public Object replace(Object original, Object target, Object owner)
            throws HibernateException {
        return original;
    }

    @Override
    public abstract Class<T> returnedClass();

    @Override
    public int[] sqlTypes() {
        return new int[]{Types.INTEGER};
    }

}
