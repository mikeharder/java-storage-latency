package com.test;

import java.io.ByteArrayInputStream;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

/**
 * Hello world!
 *
 */
public class App {
    private static final String CONTAINER_NAME = "test-" + UUID.randomUUID().toString();
    private static final byte[] CONTENT = "Hello World!".getBytes();

    public static void main(String[] args) throws Exception {
        String connectionString = System.getenv("STORAGE_CONNECTION_STRING");
        if (connectionString == null || connectionString.length() == 0) {
            System.out.println("Environment variable STORAGE_CONNECTION_STRING must be set");
            System.exit(1);
        }

        long appStartNanoTime = System.nanoTime();

        int concurrency = args.length >= 1 ? Integer.parseInt(args[0]) : 1;
        int requests = args.length >= 2 ? Integer.parseInt(args[1]) : 100;

        System.out.println("Concurrency: " + concurrency);
        System.out.println("Requests: " + requests);

        BlobServiceClient serviceClient = new BlobServiceClientBuilder().connectionString(connectionString)
                .buildClient();

        BlobContainerClient containerClient = serviceClient.getBlobContainerClient(CONTAINER_NAME);
        try {
            containerClient.create();

            AtomicInteger requestsStarted = new AtomicInteger();

            ForkJoinPool forkJoinPool = new ForkJoinPool(concurrency);
            forkJoinPool.submit(() -> {
                IntStream.range(0, concurrency).parallel()
                        .forEach(i -> sendRequests(containerClient, requestsStarted, requests, appStartNanoTime, i));
            }).get();
        } finally {
            containerClient.delete();
        }
    }

    private static void sendRequests(BlobContainerClient containerClient, AtomicInteger requestsStarted, int requests,
            long appStartNanoTime, int concurrencyId) {
        int currentRequest;
        while ((currentRequest = requestsStarted.getAndIncrement()) < requests) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(CONTENT);
            BlobClient blobClient = containerClient.getBlobClient("test-" + currentRequest);

            long startNanoTime = System.nanoTime();
            blobClient.upload(inputStream, CONTENT.length);
            long endNanoTime = System.nanoTime();

            long appStartMs = (endNanoTime - appStartNanoTime) / 1000000;
            long requestMs = (endNanoTime - startNanoTime) / 1000000;

            System.out.printf("%d\t%d\t%d\t%d\r\n", concurrencyId, Thread.currentThread().getId(), appStartMs,
                    requestMs);
        }
    }
}
