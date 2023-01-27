package de.turing85;

import java.sql.Timestamp;
import java.util.Optional;
import javax.sql.DataSource;
import lombok.Getter;
import lombok.Setter;
import org.apache.camel.CamelContext;
import org.apache.camel.processor.idempotent.jdbc.JdbcOrphanLockAwareIdempotentRepository;

@Getter
@Setter
class CustomJdbcMessageIdRepository extends JdbcOrphanLockAwareIdempotentRepository {
  private String updateDoneString;
  private String orphanRemovalString;

  public CustomJdbcMessageIdRepository(
      DataSource dataSource,
      String processorName,
      CamelContext camelContext) {
    super(dataSource, processorName, camelContext);
    setCreateString("""
        CREATE TABLE CAMEL_MESSAGEPROCESSED (
            processorName VARCHAR(255),
            messageId VARCHAR(100),
            createdAt TIMESTAMP,
            done BOOLEAN DEFAULT false,
            PRIMARY KEY (processorName, messageId)
        )
        """);
    setInsertString("""
        INSERT INTO CAMEL_MESSAGEPROCESSED (
            processorName,
            messageId,
            createdAt)
        VALUES (?, ?, ?)
        """);
    setUpdateDoneString("""
        UPDATE CAMEL_MESSAGEPROCESSED
        SET done = 'true', createdAt = ?
        WHERE
            processorName = ? AND
            messageId = ?
        """);
    setQueryString("""
        SELECT COUNT(*)
        FROM CAMEL_MESSAGEPROCESSED
        WHERE
            processorName = ? AND
            messageId = ?
        """);
    // get the default delete string, extend the condition to only outdated and unfinished entries
    setOrphanRemovalString(getDeleteString() + " AND done = 'false' AND createdAt <= ?");
    // we never want to remove anything from the table. Thus, we construct a query that will never
    // remove anything
    setDeleteString("""
        DELETE FROM CAMEL_MESSAGEPROCESSED
        WHERE
            false AND
            ? = ?
        """);
  }

  @Override
  protected void doInit() throws Exception {
    super.doInit();
    Optional.ofNullable(getTableName()).ifPresent(tableName -> {
      updateDoneString = updateDoneString.replace(DEFAULT_TABLENAME, tableName);
      orphanRemovalString = orphanRemovalString.replace(DEFAULT_TABLENAME, tableName);
    });
  }

  @Override
  protected int insert(String key) {
    // delete orphaned locks
    Timestamp xMillisAgo = new Timestamp(System.currentTimeMillis() - getLockMaxAgeMillis());
    jdbcTemplate.update(
        getOrphanRemovalString(),
        processorName,
        key,
        xMillisAgo);
    return super.insert(key);
  }

  @Override
  protected int queryForInt(String key) {
    // If the entry is {@code done}, the lock is never orphaned.
    //
    // If the update timestamp time is more than lockMaxAge then assume that the lock is orphan and
    // the process which had acquired the lock has died.
    String orphanLockRecoverQueryString =
        getQueryString() + " AND (done = 'true' OR createdAt >= ?)";
    Timestamp xMillisAgo = new Timestamp(System.currentTimeMillis() - getLockMaxAgeMillis());
    return Optional.ofNullable(jdbcTemplate.queryForObject(
        orphanLockRecoverQueryString,
        Integer.class,
        processorName,
        key,
        xMillisAgo)).orElseThrow();
  }

  @Override
  public boolean confirm(String key) {
    if (super.confirm(key)) {
      // set the "done"-flag in database
      jdbcTemplate.update(
          getUpdateDoneString(),
          new Timestamp(System.currentTimeMillis()),
          getProcessorName(),
          key);
      // the parent class has a set of message-ids and processor names that is uses to keep entries
      // alive. We can remove an entry by calling the delete-message. This is the reason why we set
      // the deleteString to a query that will never delete anything, because the parent's
      // delete(...) will execute this query string
      delete(key);
      return true;
    }
    return false;
  }
}
