package de.turing85.config;

import io.smallrye.config.ConfigMapping;
import java.time.Duration;

@ConfigMapping(prefix = "adapter.idempotency")
public interface IdempotencyConfig {
  String tableName();
  Duration lockMaxAge();
  Duration keepAliveInterval();
}
