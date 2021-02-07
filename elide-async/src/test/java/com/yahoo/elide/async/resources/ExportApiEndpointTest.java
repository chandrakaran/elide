/*
 * Copyright 2021, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.async.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.yahoo.elide.async.resources.ExportApiEndpoint.ExportApiProperties;
import com.yahoo.elide.async.service.storageengine.FileResultStorageEngine;
import com.yahoo.elide.async.service.storageengine.ResultStorageEngine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.reactivex.Observable;

import java.io.IOException;
import java.util.concurrent.Executors;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

/**
 * ExportAPiEndpoint Test.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExportApiEndpointTest {

    private ExportApiEndpoint endpoint;
    private ResultStorageEngine engine;
    private AsyncResponse asyncResponse;
    private HttpServletResponse response;
    private ExportApiProperties exportApiProperties;
    private ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);

    @BeforeEach
    public void setup() {
        engine = mock(FileResultStorageEngine.class);
        asyncResponse = mock(AsyncResponse.class);
        response = mock(HttpServletResponse.class);
    }

    @Test
    public void testGet() throws InterruptedException, IOException {
        String queryId = "1";
        when(engine.getResultsByID(queryId)).thenReturn(Observable.just("result"));

        exportApiProperties = new ExportApiProperties(Executors.newFixedThreadPool(1), 10);
        endpoint = new ExportApiEndpoint(engine, exportApiProperties);
        endpoint.get(queryId, response, asyncResponse);

        verify(engine, Mockito.times(1)).getResultsByID(queryId);
        // Sleep to let the request finish.
        Thread.sleep(100000);

        Mockito.verify(asyncResponse).resume(responseCaptor.capture());
        final Response res = responseCaptor.getValue();

        assertEquals(res.getStatus(), 200);
    }
}
