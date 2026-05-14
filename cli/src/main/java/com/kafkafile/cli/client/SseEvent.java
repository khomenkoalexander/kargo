package com.kafkafile.cli.client;

public record SseEvent(String type, String data) {}
