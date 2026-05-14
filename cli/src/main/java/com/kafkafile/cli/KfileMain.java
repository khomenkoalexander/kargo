package com.kafkafile.cli;

import com.kafkafile.cli.command.CancelCommand;
import com.kafkafile.cli.command.ReceiveCommand;
import com.kafkafile.cli.command.SendCommand;
import com.kafkafile.cli.config.CliConfigLoader;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "kfile",
    mixinStandardHelpOptions = true,
    version = "kfile 1.0.0",
    description = "Transfer files over Kafka via a relay backend.",
    subcommands = {
        SendCommand.class,
        ReceiveCommand.class,
        CancelCommand.class
    }
)
public class KfileMain implements Runnable {

    public static void main(String[] args) {
        int exit = new CommandLine(new KfileMain()).execute(args);
        System.exit(exit);
    }

    @Override
    public void run() {
        CliConfigLoader.ensureConfigFile();
        CommandLine.usage(this, System.out);
    }
}
