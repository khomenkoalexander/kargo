package com.kafkafile.cli.command;

import com.kafkafile.cli.client.BackendClient;
import com.kafkafile.cli.config.CliConfig;
import com.kafkafile.cli.config.CliConfigLoader;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "cancel", description = "Cancel an in-progress or offered transfer.")
public class CancelCommand implements Runnable {

    @Parameters(index = "0", description = "Transfer ID to cancel")
    private String transferId;

    @Option(names = "--backend-url", paramLabel = "<url>",
            description = "Backend URL (default: from config file, fallback: http://localhost:8080)")
    private String backendUrlOverride;

    @Option(names = "--token", paramLabel = "<token>",
            description = "Bearer token (default: from config file)")
    private String tokenOverride;

    @Override
    public void run() {
        CliConfig cfg = CliConfigLoader.load();
        if (backendUrlOverride != null) cfg.setBackendUrl(backendUrlOverride);
        if (tokenOverride != null) cfg.setBearerToken(tokenOverride);

        if (cfg.getBearerToken() == null) {
            System.err.println("bearer-token is required");
            System.exit(1);
        }

        BackendClient client = new BackendClient(cfg.getBackendUrl(), cfg.getBearerToken());
        try {
            client.cancel(transferId);
            System.out.println("Canceled: " + transferId);
        } catch (Exception e) {
            System.err.println("Cancel failed: " + e.getMessage());
            System.exit(1);
        }
    }
}
