CREATE TABLE idempotency (
    serviceName VARCHAR(255),
    processorName VARCHAR(255),
    messageId VARCHAR(100),
    createdAt TIMESTAMP,
    done BOOLEAN DEFAULT false,
    PRIMARY KEY (serviceName, processorName, messageId)
);