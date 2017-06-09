package com.wttttt.concurrency;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang/hadoop_inaction
 * Date: 2017-06-06
 * Time: 11:06
 */
public class Crawler implements Runnable{
    public void run(){
        String url;
        try{
            // if getUrl() returns null, then exit, so it's important to set the proper timeout
            while ((url = getUrl()) != null) {
                logger.info("Handling: " + url);
                handling(url);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static Logger logger = Logger.getLogger("BbsCrawler");

    private static BlockingQueue<String> blockingQueue = new LinkedBlockingQueue<String>();

    private static Set<String> visited = Collections.synchronizedSet(new HashSet<String>());

    private String getUrl() throws Exception{
        TimeUnit timeUnit = TimeUnit.MICROSECONDS;
        // poll from the urlQueue, timeout = 5ms  TODO: Need timeout here? Is 5ms suitable?
        String url = blockingQueue.poll(5, timeUnit);
        return url;
    }

    private void putUrl(String urlToOffer) {
        if(visited.contains(urlToOffer)) {
            logger.info("Duplicate detected: " + urlToOffer);
            return;
        }
        boolean ok = blockingQueue.offer(urlToOffer);
        if (ok == false) {
            logger.warning("Offer failed: " + urlToOffer);
        } else{
            visited.add(urlToOffer);
        }
    }

    private void downloadMp3(String mp3Url) throws Exception{
        InputStream in = new URL(mp3Url).openStream();
        String[] url = mp3Url.split("/");
        String fileName = url[url.length - 1];
        FileOutputStream fileOutputStream = new FileOutputStream("output/" + fileName);

        byte[] bb = new byte[1024];   // cache
        int len;
        while((len = in.read(bb)) > 0) {
            fileOutputStream.write(bb, 0, len);
        }
        fileOutputStream.close();
        in.close();
    }

    private void jsonHandling(String requestUrl) throws Exception{
        // 1. get the returned json
        InputStream response = new URL(requestUrl).openStream();

        Scanner scanner = new Scanner(response);
        String responseBody = scanner.useDelimiter("\\A").next();

        // 2. parse the json
        // 2.1 construct json object from the string
        JSONObject obj = new JSONObject(responseBody);
        // 2.2 check if the status is true
        boolean status = obj.getBoolean("status");
        if (!status) return;
        // 2.3 get the main content
        JSONArray arr = obj.getJSONObject("data").getJSONArray("children");
        for(int i = 0; i < arr.length(); i++) {
            String mp3Address = arr.getJSONObject(i).getString("mp3");
            if (mp3Address.equals("")) continue;
            putUrl(mp3Address);
        }
    }

    private void handling(String url) throws Exception{
        logger.info("Running thread: " + Thread.currentThread().getName());
        if (url.endsWith(".mp3")) {
            downloadMp3(url);
        } else if (url.contains("json")){
            jsonHandling(url);
        } else {
            logger.warning("Unexpected url: " + url);
        }
    }


    public static void main(String[] args) {
        int endId = 100;
        if (args.length > 0) {
            endId = Integer.valueOf(args[0]);
        }
        // 1. put the start url to the heap
        for(int i = 1; i < endId; i++) {
            String requestUrl = "The URLAdd that u want to crawler";
            Crawler.blockingQueue.offer(requestUrl);
            Crawler.visited.add(requestUrl);
        }

        // 2. multi-thread, thread.run()
        for(int i = 0; i < 5; i++) {
            Crawler crawler = new Crawler();
            new Thread(crawler, "thread" + i).start();
        }

    }
}
