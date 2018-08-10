package com.dataparse.server.service;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import com.mongodb.gridfs.GridFS;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import javax.annotation.PostConstruct;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractMongoRepository {

    public static final String MONGO_URI = "MONGO_URI";
    public static final String DB_NAME = "MONGO_DB_NAME";
    public static final String LOCAL_DB = "dataparse";

    protected GridFS gridFS;
    protected DB database;

    public static String getDbUri() {
        String uriString = System.getenv(MONGO_URI);
        if (uriString == null) {
            String localDbName = getDbName();
            // uriString = "mongodb://admin:admin@localhost:27017/" + localDbName + "?authSource=admin";
            uriString = "mongodb://localhost:27017/" + localDbName;
        }
        return uriString;
    }

    public static String getDbName() {
        String localDbName = System.getProperty(DB_NAME);

        if (localDbName == null) {
            localDbName = System.getenv(DB_NAME);
            if (localDbName == null) {
                localDbName = LOCAL_DB;
            }
        }
        return localDbName;
    }

    public static DB getDB() {
        MongoClient mongoClient;
        try {
            String uriString = getDbUri();
            MongoClientURI uri = new MongoClientURI(uriString);
            mongoClient = new MongoClient(uri);
            return mongoClient.getDB(getDbName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @PostConstruct
    public void init() throws UnknownHostException {
        database = getDB();
        gridFS = new GridFS(database);
    }


    protected DBObject getSort(PageRequest request) {
        DBObject sort = new BasicDBObject();
        if (request.getSort() != null) {
            request
                    .getSort()
                    .forEach(order -> sort.put(order.getProperty(),
                                               order.getDirection() == Sort.Direction.ASC ? 1 : -1));
        }
        return sort;
    }
}
