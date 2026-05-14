package com.kafkafile.cli.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Loads CLI configuration.
 * Priority: command-line overrides > environment variables > config file > defaults.
 */
public class CliConfigLoader {

    private static final String DEFAULT_CONFIG = String.join(System.lineSeparator(),
        "# kfile configuration",
        "# All values can also be set via environment variables (KFILE_NAME, KFILE_BEARER_TOKEN, etc.)",
        "# or overridden per-invocation with command-line flags.",
        "",
        "# --- Required ---",
        "",
        "# Your logical name on this machine. Others send files to you using this name.",
        "# name=alice",
        "",
        "# Bearer token — must match KFILE_BEARER_TOKEN on the backend.",
        "# bearer-token=changeme",
        "",
        "# --- Connection ---",
        "",
        "# Backend URL (default matches the bundled Docker Compose setup).",
        "backend-url=http://localhost:8080",
        "",
        "# --- Transfer tuning ---",
        "",
        "# Maximum chunk size in bytes (must not exceed broker max.message.bytes, default 1 MB).",
        "chunk-size=1048576",
        "",
        "# Number of chunks in flight before waiting for receiver acknowledgement.",
        "window=10",
        "",
        "# --- Receive defaults ---",
        "",
        "# Directory where received files are saved (default: current working directory).",
        "# out-dir=/home/alice/downloads",
        "",
        "# Set to true to accept all incoming offers without an interactive prompt.",
        "# auto=false",
        ""
    );

    /**
     * Ensures the config file exists, creating it with defaults if not.
     * Safe to call at any time; errors are printed to stderr and suppressed.
     */
    public static void ensureConfigFile() {
        Path configFile = resolveConfigFile();
        if (!configFile.toFile().exists()) {
            createDefaultConfig(configFile);
        }
    }

    public static CliConfig load() {
        CliConfig cfg = new CliConfig();
        loadFromFile(cfg);
        loadFromEnv(cfg);
        return cfg;
    }

    private static void loadFromFile(CliConfig cfg) {
        Path configFile = resolveConfigFile();
        if (!configFile.toFile().exists()) {
            createDefaultConfig(configFile);
            return;
        }

        try (var is = new FileInputStream(configFile.toFile())) {
            Properties p = new Properties();
            p.load(is);
            if (p.containsKey("name"))         cfg.setName(p.getProperty("name"));
            if (p.containsKey("backend-url"))  cfg.setBackendUrl(p.getProperty("backend-url"));
            if (p.containsKey("bearer-token")) cfg.setBearerToken(p.getProperty("bearer-token"));
            if (p.containsKey("chunk-size"))   cfg.setChunkSize(Integer.parseInt(p.getProperty("chunk-size")));
            if (p.containsKey("window"))       cfg.setWindow(Integer.parseInt(p.getProperty("window")));
            if (p.containsKey("out-dir"))      cfg.setOutDir(p.getProperty("out-dir"));
            if (p.containsKey("auto"))         cfg.setAuto(Boolean.parseBoolean(p.getProperty("auto")));
        } catch (IOException | NumberFormatException e) {
            System.err.println("Warning: failed to load config file " + configFile + ": " + e.getMessage());
        }
    }

    private static void createDefaultConfig(Path configFile) {
        try {
            Files.createDirectories(configFile.getParent());
            Files.writeString(configFile, DEFAULT_CONFIG, StandardCharsets.UTF_8);
            System.out.println("Created default config file: " + configFile.toAbsolutePath());
            System.out.println("Edit it to set your 'name' and 'bearer-token' before using kfile.");
        } catch (IOException e) {
            System.err.println("Warning: could not create default config file " + configFile + ": " + e.getMessage());
        }
    }

    private static void loadFromEnv(CliConfig cfg) {
        String val;
        if ((val = System.getenv("KFILE_NAME")) != null)         cfg.setName(val);
        if ((val = System.getenv("KFILE_BACKEND_URL")) != null)  cfg.setBackendUrl(val);
        if ((val = System.getenv("KFILE_BEARER_TOKEN")) != null) cfg.setBearerToken(val);
        if ((val = System.getenv("KFILE_CHUNK_SIZE")) != null)   cfg.setChunkSize(Integer.parseInt(val));
        if ((val = System.getenv("KFILE_WINDOW")) != null)       cfg.setWindow(Integer.parseInt(val));
        if ((val = System.getenv("KFILE_OUT_DIR")) != null)      cfg.setOutDir(val);
        if ((val = System.getenv("KFILE_AUTO")) != null)         cfg.setAuto(Boolean.parseBoolean(val));
    }

    public static Path resolveConfigFile() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String home = System.getProperty("user.home", ".");
        if (os.contains("win")) {
            String appData = System.getenv("APPDATA");
            String base = (appData != null) ? appData : home;
            return Paths.get(base, "kfile", "config.properties");
        }
        return Paths.get(home, ".kfile", "config.properties");
    }
}
