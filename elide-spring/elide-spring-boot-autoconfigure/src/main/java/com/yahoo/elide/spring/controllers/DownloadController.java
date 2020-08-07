/*
 * Copyright 2020, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */

package com.yahoo.elide.spring.controllers;

import com.yahoo.elide.async.models.AsyncQuery;
import com.yahoo.elide.async.models.ResultFormatType;
import com.yahoo.elide.async.service.ResultStorageEngine;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Blob;
import java.sql.SQLException;

import javax.servlet.http.HttpServletResponse;
import javax.sql.rowset.serial.SerialBlob;

@Slf4j
@Configuration
@RestController
public class DownloadController {

    private ResultStorageEngine resultStorageEngine;
    private AsyncQuery asyncQuery;

    @Autowired
    public DownloadController(ResultStorageEngine resultStorageEngine, AsyncQuery asyncQuery) {
        log.debug("Started ~~");
        this.resultStorageEngine = resultStorageEngine;
        this.asyncQuery = asyncQuery;

    }

    @GetMapping("/download/file")
    public void download(HttpServletResponse response) throws SQLException, IOException {

        ///************* Getresults from ResultStorageEngine
        byte[] temp = resultStorageEngine.getResultsByID(asyncQuery.getId());
        Blob blob = new SerialBlob(temp);
        String reconstructedStr = new String(blob.getBytes((long) 1, (int) blob.length()));
        PrintWriter writer = response.getWriter();

        if (asyncQuery.getResultFormatType() == ResultFormatType.CSV) {
            response.setContentType("text/csv");
            response.setHeader("Content-Disposition", "attachment; file=file.csv");

            ///************* Writing to the writer
            String[] arrOfStr = reconstructedStr.split("\n");

            for (int i = 0; i < arrOfStr.length; i++) {
                String s1 = arrOfStr[i];
                String s2 = s1.substring(1, s1.length() - 1) + "\n";
                writer.write(s2);
            }
        } else if (asyncQuery.getResultFormatType() == ResultFormatType.JSON) {
            response.setContentType("application/json");
            response.setHeader("Content-Disposition", "attachment; file=file.json");
            writer.write(reconstructedStr);
        }

    }
}
