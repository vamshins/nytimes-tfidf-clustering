package com.unm.app.nyt;

/**
*
* @author Vamshi Krishna N S
*
*/


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;

public class NewAbstractsAcquisition {

    public static void main(String[] args) {
        try {
            String API_KEY = "5b153a6d106bd21d308fd12d3e0fd417:15:69898101";

            String fileName = "abstract_full_" + (new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss")).format(Calendar.getInstance().getTime());
            System.out.println(fileName);
            File file = null;
            file = createFile(fileName);

            // Total articles to be retrieved
            int numberOfResults = 40000;
            // Sets the starting point of the result set.
            int currentOffset = 0;

            int offset_increment = 20;

            // Limits the number of results. 1-20.
            int limit = 20;
            boolean isFailed = false;
            int count = 0;
            while (currentOffset < numberOfResults) {
                isFailed = false;
                Set<String> lines = new HashSet<>(50000); // maybe should be bigger
                String strUrl;
                String strAbstractNewsItem;

                try {
                    URL url = new URL(getBaseURL(currentOffset, limit, API_KEY));

                    try (InputStream is = url.openStream();
                            JsonReader rdr = Json.createReader(is)) {
                        JsonObject obj = rdr.readObject();
                        JsonArray results = obj.getJsonArray("results");
                        for (JsonObject result : results.getValuesAs(JsonObject.class)) {
                            count++;
                            strUrl = result.getJsonString("url").toString();
                            strAbstractNewsItem = result.getJsonString("abstract").toString();
                            if (lines.add(strUrl)) {
                                appendTextToFile(file, count + "|" + strUrl + "|" + strAbstractNewsItem + "\n");
                            } else {
                                System.out.println("Duplicate Entry Found at " + count + ", URL: " + strUrl);
                            }
                        }
                    } catch (IOException ex) {
                        System.out.println("IOException : " + ex.getMessage());
                        isFailed = true;
                    }
                    if (!isFailed) {
                        currentOffset += offset_increment;
                    }
                    System.out.println("Number of articles retrieved : " + currentOffset);
                } catch (UniformInterfaceException ex) {
                    System.out.println("UniformInterfaceException : " + ex.getMessage());
                    continue;
                } catch (ClientHandlerException ex) {
                    System.out.println("ClientHandlerException : " + ex.getMessage());
                }
            }
        } catch (IOException ex) {
            System.out.println("IOException : " + ex.getMessage());
        }
    }

    private static String getBaseURL(int currentOffset, int limit, String API_KEY) {
        // Ex: "http://api.nytimes.com/svc/news/v3/content/all/all/.json?limit=20&offset=0&api-key=5b153a6d106bd21d308fd12d3e0fd417:15:69898101");

        StringBuffer baseUrl = new StringBuffer("http://api.nytimes.com/svc/news/v3/content/all/all/.json");
        baseUrl = baseUrl.append("?limit=").append(limit).append("&offset=").append(currentOffset).append("&api-key=").append(API_KEY);
        return baseUrl.toString();
    }

    static File createFile(String fileName) throws IOException {
        File file = new File("E:/Temp/" + fileName + ".csv");

        //if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }
        return file;
    }
    
    static void appendTextToFile(File file, String article) throws IOException {
        FileWriter fileWriter = new FileWriter(file, true);
        try (BufferedWriter bufferWritter = new BufferedWriter(fileWriter)) {
            bufferWritter.write(article);
        }
    }
}
