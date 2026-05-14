package com.kafkafile.cli.config;

public class CliConfig {

    private String name;
    private String backendUrl = "http://localhost:8080";
    private String bearerToken;
    private int chunkSize = 1_048_576;  // 1 MB
    private int window = 10;
    private String outDir = ".";
    private boolean auto = false;

    public String getName() { return name; }
    public void setName(String v) { this.name = v; }

    public String getBackendUrl() { return backendUrl; }
    public void setBackendUrl(String v) { this.backendUrl = v; }

    public String getBearerToken() { return bearerToken; }
    public void setBearerToken(String v) { this.bearerToken = v; }

    public int getChunkSize() { return chunkSize; }
    public void setChunkSize(int v) { this.chunkSize = v; }

    public int getWindow() { return window; }
    public void setWindow(int v) { this.window = v; }

    public String getOutDir() { return outDir; }
    public void setOutDir(String v) { this.outDir = v; }

    public boolean isAuto() { return auto; }
    public void setAuto(boolean v) { this.auto = v; }

    public void validate() {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("'name' is required");
        if (backendUrl == null || backendUrl.isBlank()) throw new IllegalArgumentException("'backend-url' is required");
        if (bearerToken == null || bearerToken.isBlank()) throw new IllegalArgumentException("'bearer-token' is required");
    }
}
