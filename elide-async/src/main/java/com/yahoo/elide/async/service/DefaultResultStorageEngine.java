/*
 * Copyright 2020, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */

package com.yahoo.elide.async.service;

import com.yahoo.elide.Elide;
import com.yahoo.elide.async.models.AsyncQueryResultStorage;
import com.yahoo.elide.core.DataStore;
import com.yahoo.elide.request.EntityProjection;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import javax.inject.Singleton;
import javax.sql.rowset.serial.SerialBlob;

/**
 * Utility class which implements ResultStorageEngine.
 */
@Singleton
@Slf4j
@Getter
public class DefaultResultStorageEngine implements ResultStorageEngine {

    @Setter private Elide elide;
    @Setter private DataStore dataStore;
    private String baseURL;

    public DefaultResultStorageEngine() {
    }

    public DefaultResultStorageEngine(Elide elide, DataStore dataStore, String baseURL) {
        this.elide = elide;
        this.dataStore = dataStore;
        this.baseURL = baseURL;
    }

    @Override
    public URL storeResults(String asyncQueryID, String responseBody) {
        byte[] temp = responseBody.getBytes();
        Blob response = null;
        try {
            response = new SerialBlob(temp);
        } catch (SQLException e) {
            log.error("Exception: {}", e);
            throw new IllegalStateException(e);
        }
        AsyncQueryResultStorage asyncQueryResultStorage = new AsyncQueryResultStorage();
        asyncQueryResultStorage.setId(asyncQueryID);
        asyncQueryResultStorage.setResult(response);

        URL url = null;
        String buildURL = baseURL + "/AsyncQueryResultStorage/" + asyncQueryID;
        try {
            url = new URL(buildURL);
        } catch (MalformedURLException e) {
            log.error("Exception: {}", e);
            throw new IllegalStateException(e);
        }

        return url;

    }

    @Override
    public AsyncQueryResultStorage getResultsByID(String asyncQueryID) {
        log.debug("getResultsByID");

        AsyncQueryResultStorage asyncQueryResultStorages = null;

        try {
            asyncQueryResultStorages = (AsyncQueryResultStorage) EIT.executeInTransaction(elide,
                    dataStore, (tx, scope) -> {

                EntityProjection asyncQueryCollection = EntityProjection.builder()
                        .type(AsyncQueryResultStorage.class)
                        .build();

                Object loaded = tx.loadObject(asyncQueryCollection, asyncQueryID, scope);

                return loaded;


            });
        } catch (Exception e) {
            log.error("Exception: {}", e);
        }
        return asyncQueryResultStorages;
    }

    @Override
    public AsyncQueryResultStorage deleteResultsByID(String asyncQueryID) {
        log.debug("deleteResultsByID");

        AsyncQueryResultStorage asyncQueryResultStorages = null;

        try {
            asyncQueryResultStorages = (AsyncQueryResultStorage) EIT.executeInTransaction(elide,
                    dataStore, (tx, scope) -> {

                EntityProjection asyncQueryCollection = EntityProjection.builder()
                        .type(AsyncQueryResultStorage.class)
                        .build();

                Object loaded = tx.loadObject(asyncQueryCollection, asyncQueryID, scope);
                Object result = loaded;

                if (result != null) {
                    tx.delete(result, scope);
                }

                return loaded;
            });
        } catch (Exception e) {
            log.error("Exception: {}", e);
        }
        return asyncQueryResultStorages;
    }

    @Override
    public Collection<AsyncQueryResultStorage> deleteAllResults() {
        log.debug("deleteAllResults");

        Collection<AsyncQueryResultStorage> asyncQueryResultStorages = null;

        try {
            asyncQueryResultStorages = (Collection<AsyncQueryResultStorage>) EIT.executeInTransaction(elide, dataStore,
                    (tx, scope) -> {

                        EntityProjection asyncQueryCollection = EntityProjection.builder()
                                .type(AsyncQueryResultStorage.class)
                                .build();

                        Iterable<Object> loaded = tx.loadObjects(asyncQueryCollection, scope);
                        Iterator<Object> itr = loaded.iterator();

                        while (itr.hasNext()) {
                            AsyncQueryResultStorage query = (AsyncQueryResultStorage) itr.next();
                            if (query != null) {
                                tx.delete(query, scope);
                            }
                        }
                        return loaded;
                    });
        } catch (Exception e) {
            log.error("Exception: {}", e);
        }
        return asyncQueryResultStorages;
    }
}
