package com.vk.internship;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class KVServer {
    private static final Logger log = LoggerFactory.getLogger(KVServer.class);
    private final Server server;

    public KVServer(int port, TarantoolDao dao) {
        this.server = NettyServerBuilder.forPort(port)
                .addService(new KVServiceImpl(dao))
                .build();
    }

    public void start() throws IOException {
        server.start();
        log.info("gRPC server started on port {}", server.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                KVServer.this.stop();
            } catch (InterruptedException e) {
                log.error("Shutdown interrupted", e);
            }
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            log.info("gRPC server stopped");
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws Exception {
        TarantoolConfig config = TarantoolConfig.fromEnv();
        try (TarantoolDao dao = new TarantoolDao(config)) {
            KVServer server = new KVServer(8080, dao);
            server.start();
            server.blockUntilShutdown();
        } catch (Exception e) {
            System.err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
