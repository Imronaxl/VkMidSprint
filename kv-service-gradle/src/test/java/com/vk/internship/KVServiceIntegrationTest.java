package com.vk.internship;

import com.google.protobuf.ByteString;
import com.vk.internship.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class KVServiceIntegrationTest {

    private static GenericContainer<?> tarantool;
    private static TarantoolDao dao;
    private static ManagedChannel channel;
    private static KVServiceGrpc.KVServiceBlockingStub blockingStub;
    private static KVServiceGrpc.KVServiceStub asyncStub;

    @BeforeAll
    static void setUp() throws Exception {
        tarantool = new GenericContainer<>(DockerImageName.parse("tarantool/tarantool:3.2"))
                .withExposedPorts(3301);
        tarantool.start();

        TarantoolConfig config = new TarantoolConfig(
                tarantool.getHost(),
                tarantool.getMappedPort(3301),
                null, null
        );
        dao = new TarantoolDao(config);

        String serverName = InProcessServerBuilder.generateName();
        io.grpc.Server server = InProcessServerBuilder.forName(serverName)
                .addService(new KVServiceImpl(dao))
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        blockingStub = KVServiceGrpc.newBlockingStub(channel);
        asyncStub = KVServiceGrpc.newStub(channel);
    }

    @AfterAll
    static void tearDown() throws Exception {
        channel.shutdown();
        dao.close();
        tarantool.stop();
    }

    @BeforeEach
    void clean() throws Exception {
        dao.truncate();
    }

    @Test
    void testPutAndGet() throws Exception {
        blockingStub.put(PutRequest.newBuilder()
                .setKey("k1")
                .setValue(ByteString.copyFromUtf8("v1"))
                .build());
        GetResponse resp = blockingStub.get(GetRequest.newBuilder().setKey("k1").build());
        assertTrue(resp.hasValue());
        assertEquals("v1", resp.getValue().toStringUtf8());

        blockingStub.put(PutRequest.newBuilder().setKey("k2").build());
        GetResponse respNull = blockingStub.get(GetRequest.newBuilder().setKey("k2").build());
        assertFalse(respNull.hasValue());

        GetResponse missing = blockingStub.get(GetRequest.newBuilder().setKey("missing").build());
        assertFalse(missing.hasValue());
    }

    @Test
    void testDelete() throws Exception {
        blockingStub.put(PutRequest.newBuilder().setKey("del").setValue(ByteString.copyFromUtf8("x")).build());
        assertTrue(blockingStub.get(GetRequest.newBuilder().setKey("del").build()).hasValue());

        blockingStub.delete(DeleteRequest.newBuilder().setKey("del").build());
        GetResponse after = blockingStub.get(GetRequest.newBuilder().setKey("del").build());
        assertFalse(after.hasValue());
    }

    @Test
    void testCount() throws Exception {
        assertEquals(0, blockingStub.count(CountRequest.getDefaultInstance()).getCount());
        blockingStub.put(PutRequest.newBuilder().setKey("a").build());
        blockingStub.put(PutRequest.newBuilder().setKey("b").setValue(ByteString.EMPTY).build());
        assertEquals(2, blockingStub.count(CountRequest.getDefaultInstance()).getCount());
    }

    @Test
    void testRange() throws Exception {
        blockingStub.put(PutRequest.newBuilder().setKey("a").setValue(ByteString.copyFromUtf8("1")).build());
        blockingStub.put(PutRequest.newBuilder().setKey("b").setValue(ByteString.copyFromUtf8("2")).build());
        blockingStub.put(PutRequest.newBuilder().setKey("c").setValue(ByteString.copyFromUtf8("3")).build());
        blockingStub.put(PutRequest.newBuilder().setKey("d").setValue(ByteString.copyFromUtf8("4")).build());
        blockingStub.put(PutRequest.newBuilder().setKey("e").setValue(ByteString.copyFromUtf8("5")).build());

        final CountDownLatch latch1 = new CountDownLatch(1);
        List<RangeResponse> responses = new ArrayList<>();
        asyncStub.range(RangeRequest.newBuilder().setKeySince("b").setKeyTo("d").build(),
                new StreamObserver<>() {
                    @Override public void onNext(RangeResponse v) { responses.add(v); }
                    @Override public void onError(Throwable t) { latch1.countDown(); }
                    @Override public void onCompleted() { latch1.countDown(); }
                });
        assertTrue(latch1.await(5, TimeUnit.SECONDS));
        assertEquals(3, responses.size());
        assertEquals("b", responses.get(0).getKey());
        assertEquals("c", responses.get(1).getKey());
        assertEquals("d", responses.get(2).getKey());

        responses.clear();
        final CountDownLatch latch2 = new CountDownLatch(1);
        asyncStub.range(RangeRequest.newBuilder().setKeySince("c").setKeyTo("c").build(),
                new StreamObserver<>() {
                    @Override public void onNext(RangeResponse v) { responses.add(v); }
                    @Override public void onError(Throwable t) { latch2.countDown(); }
                    @Override public void onCompleted() { latch2.countDown(); }
                });
        assertTrue(latch2.await(5, TimeUnit.SECONDS));
        assertEquals(1, responses.size());
        assertEquals("c", responses.get(0).getKey());
    }

    @Test
    void testRangeInvalid() {
        assertThrows(io.grpc.StatusRuntimeException.class, () -> {
            var iterator = blockingStub.range(RangeRequest.newBuilder().setKeySince("z").setKeyTo("a").build());
            iterator.hasNext();
        });
    }

    @Test
    void testLargeRangePagination() throws Exception {
        for (int i = 0; i < 2500; i++) {
            String key = String.format("key%04d", i);
            blockingStub.put(PutRequest.newBuilder().setKey(key).build());
        }
        CountDownLatch latch = new CountDownLatch(1);
        List<RangeResponse> responses = new ArrayList<>();
        asyncStub.range(RangeRequest.newBuilder().setKeySince("key0000").setKeyTo("key2499").build(),
                new StreamObserver<>() {
                    @Override public void onNext(RangeResponse v) { responses.add(v); }
                    @Override public void onError(Throwable t) { latch.countDown(); }
                    @Override public void onCompleted() { latch.countDown(); }
                });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(2500, responses.size());
    }
}
