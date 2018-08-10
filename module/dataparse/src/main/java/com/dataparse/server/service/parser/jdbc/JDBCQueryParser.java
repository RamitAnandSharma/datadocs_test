package com.dataparse.server.service.parser.jdbc;

import com.dataparse.server.service.db.ConnectionParams;
import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.IndexedParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.dataparse.server.service.tasks.ExceptionWrapper;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.RemoteLinkDescriptor;
import com.dataparse.server.util.DbUtils;
import com.dataparse.server.util.db.DbConnectionProvider;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.postgresql.jdbc4.Jdbc4Array;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.regex.Pattern;

import static com.dataparse.server.service.parser.RecordIteratorBuilder.with;

@Slf4j
public class JDBCQueryParser extends Parser {

    @Autowired
    private DbConnectionProvider dbConnectionProvider;

    private String queryString;

    protected ConnectionParams connectionParams;
    protected Descriptor descriptor;
    protected Boolean knownCount = false;

    private Boolean preview;

    public JDBCQueryParser(RemoteLinkDescriptor descriptor, String queryString, Boolean preview) {
        this.queryString = queryString;
        this.connectionParams = descriptor.getParams();
        this.descriptor = descriptor;
        this.preview = preview;
    }

    private String trimQuery(String query){
        while(true){
            query = query.trim();
            if(query.endsWith(";")){
                query = query.substring(0, query.length() - 1);
            } else {
                break;
            }
        }
        return query;
    }

    protected String getCountQueryString(){
        return "select count(*) AS total from (" + trimQuery(this.queryString) + ") x";
    }

    protected String getQueryString(){
        String query = queryString.trim();
        if(connectionParams.getProtocol().equals(Protocol.mysql)) {
            Pattern pattern = Pattern.compile("(?i).*\\blimit\\b \\d+.*", Pattern.DOTALL);
            while(query.endsWith(";")){
                query = query.substring(0, query.length() - 1);
            }
            if (descriptor.getLimit() != null && !pattern.matcher(queryString).matches()) {
                while(query.endsWith(";")){
                    query = query.substring(0, query.length() - 1);
                }
                query = query + " LIMIT " + descriptor.getLimit();
            } else if(descriptor.getLimit() != null && preview) {
                String[] splitByLimit = query.toLowerCase().split("limit");
                int currentLimit = Integer.parseInt(splitByLimit[splitByLimit.length - 1].trim());
                if(currentLimit > descriptor.getLimit()) {
                    query = StringUtils.join(Arrays.copyOfRange(splitByLimit, 0, splitByLimit.length - 1), ' ');
                    query += " LIMIT " + descriptor.getLimit();
                }
            }
        }
        return preview && !knownCount ? query : getQueryStringWithCount(connectionParams.getProtocol(), query);
    }


    private static String getQueryStringWithCount(Protocol protocol, String query) {
        switch (protocol) {
            case mysql:
                return query;
            case oracle:
            case postgresql:
            case sqlserver:
                String countSubquery = ", count(*) over() as rank__";
                String[] split = query.split("(?i)\\bfrom\\b");
                return String.format("%s %s from %s", split[0], countSubquery, StringUtils.join(Arrays.copyOfRange(split, 1, split.length), "from"));
            default:
                throw new RuntimeException("There is no such connection params: " + protocol);
        }
    }

    @Override
    public Pair<Long, Boolean> getRowsEstimateCount(long fileSize) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try(Connection conn = dbConnectionProvider.getConnection(connectionParams)) {
            Pair<Long, Boolean> result = testQuery(conn);
            log.info("Retrieve estimated rows count took {}", stopwatch.stop());
            return result;
        } catch (Exception e) {
            throw ExceptionWrapper.wrap(e);
        }
    }

    public Pair<Long, Boolean> testQuery(Connection connection){
        String query = getCountQueryString();
        try {
            connection.setAutoCommit(false);
            connection.setReadOnly(true);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(query)){
            rs.next();
            return Pair.of(rs.getLong("total"), true);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    protected String quote(String s){
        return DbUtils.quote(connectionParams.getProtocol(), s);
    }

    @Override
    public RecordIterator parse() throws IOException {
        final boolean isMySQL = Protocol.mysql.equals(connectionParams.getProtocol());
        Connection conn = createConnection();

        log.info("Executing query: {}", queryString);

        Statement st;
        ResultSet rs;

        // completely disgusting
        Statement stToClose = null;
        ResultSet rsToClose = null;
        ResultSetMetaData rsmd;
        int columnsNumber;
        List<String> columnLabels = new ArrayList<>();
        try {
            st = DbUtils.createStatement(conn, connectionParams, descriptor.getLimit());
            stToClose = st;
            long start = System.currentTimeMillis();
            try {
                rs = st.executeQuery(getQueryString());
                rsToClose = rs;
            } finally {
                log.info("Query executed in: {}", (System.currentTimeMillis() - start));
            }
            Stopwatch defineMetaData = Stopwatch.createStarted();
            rsmd = rs.getMetaData();

            /**
             * avoid '__rank__' column for ingestion
             * */
            columnsNumber = preview || isMySQL ? rsmd.getColumnCount() : rsmd.getColumnCount() - 1;

            Multiset<String> columnNames = HashMultiset.create();
            for (int i = 1; i <= columnsNumber; i++) {
                columnNames.add(rsmd.getColumnLabel(i));
            }
            log.info("Define metadata took {}", defineMetaData.stop());
            Stopwatch defineColumnLabel = Stopwatch.createStarted();
            for (int i = 1; i <= columnsNumber; i++) {
                String columnLabel = rsmd.getColumnLabel(i);
                if(columnNames.count(columnLabel) > 1){
                    columnLabel = DbUtils.getTablePrefixedColumnName(connectionParams.getProtocol(), rsmd, i);
                }
                columnLabels.add(columnLabel);
            }
            log.info("Define column labels took {}", defineColumnLabel.stop());
        } catch (Exception e){
            String cantIssueDms = "Can not issue data manipulation statements";
            if(e.getMessage().startsWith(cantIssueDms)){
                throw new IOException(cantIssueDms);
            }
            DbUtils.close(conn, stToClose, rsToClose);
            throw new IOException(e);
        }

        return with(new RecordIterator() {

            @Override
            public void close() throws IOException {
                DbUtils.close(conn, st, rs);
            }

            private Map<AbstractParsedColumn, Object> currentObj;
            private boolean didNext = false;
            private boolean hasNext = false;
            private Long totalRowsCount = -1L;

            private Object castValueToNativeType(Object o){
                if(o instanceof Date){
                    return new Date(((Date) o).getTime());
                }
                return o;
            }

            private Map<AbstractParsedColumn, Object> nextRaw() {
                if (!didNext) {
                    try {
                        if(!(hasNext = rs.next())){
                            return null;
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }

                }
                didNext = false;
                try {
                    Map<AbstractParsedColumn, Object> o = new LinkedHashMap<>();
                    for (int i = 1; i <= columnsNumber; i++) {
                        Object obj = rs.getObject(i);
                        if(obj instanceof Jdbc4Array) {
                            Object arr = ((Jdbc4Array) obj).getArray();
                            List<Object> l = new ArrayList<>(Array.getLength(arr));
                            if (arr.getClass().isArray()) {
                                int length = Array.getLength(arr);
                                for (int j = 0; j < length; j++) {
                                    Object arrObj = Array.get(arr, j);
                                    l.add(castValueToNativeType(arrObj));
                                }
                            }
                            obj = l;
                        } else {
                            obj = castValueToNativeType(obj);
                        }
                        o.put(new IndexedParsedColumn(i - 1, columnLabels.get(i - 1)), obj);

                    }
                    if(!isMySQL && totalRowsCount == -1 && !preview && !knownCount) {
                        totalRowsCount = rs.getLong(columnsNumber + 1);
                    }
                    return o;
                }
                catch (Exception e){
                    throw new RuntimeException(e);
                }
            }

            @Override
            public long getRowsCount() {
                return totalRowsCount;
            }

            @Override
            public Map<AbstractParsedColumn, Object> getRaw() {
                return this.currentObj;
            }

            public Map<AbstractParsedColumn, Object> next(){
                this.currentObj = nextRaw();
                return this.currentObj;
            }

            private TypeDescriptor getType(int columnIdx){
                String columnTypeName;
                int columnDisplaySize;
                try {
                    columnTypeName = rsmd.getColumnTypeName(columnIdx);
                    columnDisplaySize = rsmd.getColumnDisplaySize(columnIdx);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return DbUtils.getDataType(connectionParams.getProtocol(), columnTypeName, columnDisplaySize);
            }

            @Override
            public Map<AbstractParsedColumn, TypeDescriptor> getSchema() {
                Map<AbstractParsedColumn, Object> o = getRaw();
                if(o == null){
                    return null;
                }
                Map<AbstractParsedColumn, TypeDescriptor> schema = new LinkedHashMap<>();
                int i = 0;
                for(Map.Entry<AbstractParsedColumn, Object> entry: o.entrySet()){
                    TypeDescriptor type;
                    i++;
                    if(entry.getValue() instanceof List){
                        List<Object> l = (List) entry.getValue();
                        final int j = i;
                        type = l.stream().limit(1000).map(v -> this.getType(j)).reduce(TypeDescriptor::getCommonType).orElse(null);
                    } else {
                        type = getType(i);
                    }
                    schema.put(entry.getKey(), type);
                }
                return schema;
            }

            public boolean hasNext(){
                if (!didNext) {
                    try {
                        hasNext = rs.next();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    didNext = true;
                }
                return hasNext;
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

    private Connection createConnection() {
        return dbConnectionProvider.getConnection(connectionParams, true);
    }

}
