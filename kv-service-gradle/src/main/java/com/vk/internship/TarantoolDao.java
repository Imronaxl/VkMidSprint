package com.vk.internship;

import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.mapping.TarantoolResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TarantoolDao implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TarantoolDao.class);
    private TarantoolBoxClient client;

    public TarantoolDao(TarantoolConfig config) throws Exception {
        this.client = TarantoolFactory.box()
                .withConnectTimeout(5000)
                .withReconnectAfter(5000)
                .withHost(config.getHost())
                .withPort(config.getPort())
                .withUser(config.getUsername())
                .withPassword(config.getPassword())
                .build();

        ensureSpace();
    }

    private void ensureSpace() throws ExecutionException, InterruptedException {
        String createSpaceLua = 
            "if box.space.KV == nil then " +
            "    box.schema.space.create('KV', { if_not_exists = true, format = { " +
            "        {name = 'key', type = 'string'}, " +
            "        {name = 'value', type = 'varbinary', is_nullable = true} " +
            "    } }) " +
            "    box.space.KV:create_index('primary', { type = 'tree', parts = {'key'} }) " +
            "end " +
            "return true";

        client.eval(createSpaceLua, Collections.emptyList()).get();
        log.info("Space KV is ready.");
    }

    public void put(String key, byte[] value) throws ExecutionException, InterruptedException {
        String luaScript =
            "local k, v = ...\n" +
            "return box.space.KV:replace{k, v}";

        List<Object> args = Arrays.asList(key, value);
        client.eval(luaScript, args).get();
        log.debug("Put key={}", key);
    }

    public byte[] get(String key) throws ExecutionException, InterruptedException {
        String luaScript =
            "local k = ...\n" +
            "local tuple = box.space.KV:get{k}\n" +
            "return tuple and {tuple[1], tuple[2]} or nil";

        TarantoolResponse<List<?>> response = client.eval(luaScript, Collections.singletonList(key)).get();
        List<?> result = response.get();

        if (result == null || result.isEmpty()) {
            return null;
        }

        Object firstRow = result.get(0);
        if (!(firstRow instanceof List)) {
            return null;
        }

        List<?> tuple = (List<?>) firstRow;
        if (tuple.isEmpty()) {
            return null;
        }

        Object val = tuple.size() > 1 ? tuple.get(1) : null;
        return (val == null) ? null : (byte[]) val;
    }

    public void delete(String key) throws ExecutionException, InterruptedException {
        String luaScript =
            "local k = ...\n" +
            "return box.space.KV:delete{k}";

        client.eval(luaScript, Collections.singletonList(key)).get();
        log.debug("Delete key={}", key);
    }

    public List<KeyValue> range(String keySince, String keyTo) throws ExecutionException, InterruptedException {
        if (keySince.compareTo(keyTo) > 0) {
            throw new IllegalArgumentException("key_since must be <= key_to");
        }

        String luaScript =
            "local from, to = ...\n" +
            "local result = {}\n" +
            "for _, tuple in box.space.KV.index.primary:pairs(from, {iterator = 'GE'}) do\n" +
            "    if tuple[1] > to then break end\n" +
            "    table.insert(result, {tuple[1], tuple[2]})\n" +
            "end\n" +
            "return result";

        List<Object> args = Arrays.asList(keySince, keyTo);
        TarantoolResponse<List<?>> response = client.eval(luaScript, args).get();

        List<?> rows = response.get();
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }

        if (rows.size() == 1 && rows.get(0) instanceof List) {
            rows = (List<?>) rows.get(0);
        }

        List<KeyValue> resultList = new ArrayList<>();
        for (Object rowObj : rows) {
            if (!(rowObj instanceof List)) {
                continue;
            }
            List<?> tuple = (List<?>) rowObj;
            if (tuple.isEmpty()) {
                continue;
            }

            Object kObj = tuple.get(0);
            String k = kObj == null ? null : kObj.toString();
            byte[] v = null;

            if (tuple.size() > 1 && tuple.get(1) instanceof byte[]) {
                v = (byte[]) tuple.get(1);
            } else if (tuple.size() > 1 && tuple.get(1) instanceof String) {
                v = ((String) tuple.get(1)).getBytes();
            }

            resultList.add(new KeyValue(k, v));
        }

        log.debug("Range from={} to={} returned {} items", keySince, keyTo, resultList.size());
        return resultList;
    }

    public long count() throws ExecutionException, InterruptedException {
        TarantoolResponse<List<?>> response = client.eval("return box.space.KV:len()", Collections.emptyList()).get();
        List<?> result = response.get();
        return (result == null || result.isEmpty()) ? 0L : ((Number) result.get(0)).longValue();
    }

    public void truncate() throws ExecutionException, InterruptedException {
        client.eval("box.space.KV:truncate()", Collections.emptyList()).get();
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    public static class KeyValue {
        private final String key;
        private final byte[] value;

        public KeyValue(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }
    }
}
