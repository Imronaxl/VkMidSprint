package com.vk.internship;

import com.google.protobuf.ByteString;
import com.vk.internship.grpc.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private final TarantoolDao dao;

    public KVServiceImpl(TarantoolDao dao) {
        this.dao = dao;
    }

    @Override
    public void put(PutRequest req, StreamObserver<PutResponse> resp) {
        try {
            byte[] val = req.hasValue() ? req.getValue().toByteArray() : null;
            dao.put(req.getKey(), val);
            resp.onNext(PutResponse.getDefaultInstance());
            resp.onCompleted();
        } catch (Exception e) {
            log.error("put error", e);
            resp.onError(Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void get(GetRequest req, StreamObserver<GetResponse> resp) {
        try {
            byte[] val = dao.get(req.getKey());
            GetResponse.Builder b = GetResponse.newBuilder();
            if (val != null) {
                b.setValue(ByteString.copyFrom(val));
            }
            resp.onNext(b.build());
            resp.onCompleted();
        } catch (Exception e) {
            log.error("get error", e);
            resp.onError(Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void delete(DeleteRequest req, StreamObserver<DeleteResponse> resp) {
        try {
            dao.delete(req.getKey());
            resp.onNext(DeleteResponse.getDefaultInstance());
            resp.onCompleted();
        } catch (Exception e) {
            log.error("delete error", e);
            resp.onError(Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void range(RangeRequest req, StreamObserver<RangeResponse> resp) {
        try {
            String from = req.getKeySince();
            String to = req.getKeyTo();
            if (from.compareTo(to) > 0) {
                resp.onError(Status.INVALID_ARGUMENT.withDescription("key_since <= key_to required").asRuntimeException());
                return;
            }
            for (TarantoolDao.KeyValue kv : dao.range(from, to)) {
                RangeResponse.Builder b = RangeResponse.newBuilder().setKey(kv.getKey());
                if (kv.getValue() != null) {
                    b.setValue(ByteString.copyFrom(kv.getValue()));
                }
                resp.onNext(b.build());
            }
            resp.onCompleted();
        } catch (Exception e) {
            log.error("range error", e);
            resp.onError(Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void count(CountRequest req, StreamObserver<CountResponse> resp) {
        try {
            long c = dao.count();
            resp.onNext(CountResponse.newBuilder().setCount(c).build());
            resp.onCompleted();
        } catch (Exception e) {
            log.error("count error", e);
            resp.onError(Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }
}
