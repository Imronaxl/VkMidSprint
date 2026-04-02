package com.vk.internship;

public class TarantoolConfig {
    private final String host;
    private final int port;
    private final String username;
    private final String password;

    public TarantoolConfig(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public static TarantoolConfig fromEnv() {
        String host = System.getenv().getOrDefault("TARANTOOL_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("TARANTOOL_PORT", "3301"));
        String username = System.getenv("TARANTOOL_USERNAME");
        String password = System.getenv("TARANTOOL_PASSWORD");
        return new TarantoolConfig(host, port, username, password);
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
}
