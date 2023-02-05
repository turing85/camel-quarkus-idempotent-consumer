package de.turing85.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;

@ConfigMapping(prefix = "service")
public interface ServiceConfig {
  @WithName("idempotency")
  IdempotencyConfig idempotencyConfig();

  String name();
}
