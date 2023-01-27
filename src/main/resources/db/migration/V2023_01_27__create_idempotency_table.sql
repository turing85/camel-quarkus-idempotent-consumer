CREATE TABLE idempotency (
    adapterName VARCHAR(255),
    processorName VARCHAR(255),
    messageId VARCHAR(100),
    createdAt TIMESTAMP,
    done BOOLEAN DEFAULT false,
    PRIMARY KEY (adapterName, processorName, messageId)
);