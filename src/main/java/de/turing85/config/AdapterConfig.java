package de.turing85.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;

@ConfigMapping(prefix = "adapter")
public interface AdapterConfig {
  @WithName("idempotency")
  IdempotencyConfig idempotencyConfig();

  String name();
}
