package com.hawx.entity.event.innodb;

import com.taobao.tddl.dbsync.binlog.CharsetConversion;
import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;

/**
 * A Query_log_event is created for each query that modifies the database, unless the query is
 * logged row-based. The Post-Header has five components:
 *
 * <table>
 * <caption>Post-Header for Query_log_event</caption>
 * <tr>
 * <th>Name</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>slave_proxy_id</td>
 * <td>4 byte unsigned integer</td>
 * <td>An integer identifying the client thread that issued the query. The id is
 * unique per server. (Note, however, that two threads on different servers may
 * have the same slave_proxy_id.) This is used when a client thread creates a
 * temporary table local to the client. The slave_proxy_id is used to
 * distinguish temporary tables that belong to different clients.</td>
 * </tr>
 * <tr>
 * <td>exec_time</td>
 * <td>4 byte unsigned integer</td>
 * <td>The time from when the query started to when it was logged in the binlog,
 * in seconds.</td>
 * </tr>
 * <tr>
 * <td>db_len</td>
 * <td>1 byte integer</td>
 * <td>The length of the name of the currently selected database.</td>
 * </tr>
 * <tr>
 * <td>error_code</td>
 * <td>2 byte unsigned integer</td>
 * <td>Error code generated by the master. If the master fails, the slave will
 * fail with the same error code, except for the error codes ER_DB_CREATE_EXISTS
 * == 1007 and ER_DB_DROP_EXISTS == 1008.</td>
 * </tr>
 * <tr>
 * <td>status_vars_len</td>
 * <td>2 byte unsigned integer</td>
 * <td>The length of the status_vars block of the Body, in bytes. See
 * query_log_event_status_vars "below".</td>
 * </tr>
 * </table>
 *
 * The Body has the following components:
 *
 * <table>
 * <caption>Body for Query_log_event</caption>
 * <tr>
 * <th>Name</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>query_log_event_status_vars status_vars</td>
 * <td>status_vars_len bytes</td>
 * <td>Zero or more status variables. Each status variable consists of one byte
 * identifying the variable stored, followed by the value of the variable. The
 * possible variables are listed separately in the table
 * Table_query_log_event_status_vars "below". MySQL always writes events in the
 * order defined below; however, it is capable of reading them in any order.</td>
 * </tr>
 * <tr>
 * <td>db</td>
 * <td>db_len+1</td>
 * <td>The currently selected database, as a null-terminated string. (The
 * trailing zero is redundant since the length is already known; it is db_len
 * from Post-Header.)</td>
 * </tr>
 * <tr>
 * <td>query</td>
 * <td>variable length string without trailing zero, extending to the end of the
 * event (determined by the length field of the Common-Header)</td>
 * <td>The SQL query.</td>
 * </tr>
 * </table>
 *
 * The following table lists the status variables that may appear in the status_vars field.
 * Table_query_log_event_status_vars
 *
 * <table>
 * <caption>Status variables for Query_log_event</caption>
 * <tr>
 * <th>Status variable</th>
 * <th>1 byte identifier</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>flags2</td>
 * <td>Q_FLAGS2_CODE == 0</td>
 * <td>4 byte bitfield</td>
 * <td>The flags in thd->options, binary AND-ed with OPTIONS_WRITTEN_TO_BIN_LOG.
 * The thd->options bitfield contains options for "SELECT". OPTIONS_WRITTEN
 * identifies those options that need to be written to the binlog (not all do).
 * Specifically, OPTIONS_WRITTEN_TO_BIN_LOG equals (OPTION_AUTO_IS_NULL |
 * OPTION_NO_FOREIGN_KEY_CHECKS | OPTION_RELAXED_UNIQUE_CHECKS |
 * OPTION_NOT_AUTOCOMMIT), or 0x0c084000 in hex. These flags correspond to the
 * SQL variables SQL_AUTO_IS_NULL, FOREIGN_KEY_CHECKS, UNIQUE_CHECKS, and
 * AUTOCOMMIT, documented in the "SET Syntax" section of the MySQL Manual. This
 * field is always written to the binlog in version >= 5.0, and never written in
 * version < 5.0.</td>
 * </tr>
 * <tr>
 * <td>sql_mode</td>
 * <td>Q_SQL_MODE_CODE == 1</td>
 * <td>8 byte bitfield</td>
 * <td>The sql_mode variable. See the section "SQL Modes" in the MySQL manual,
 * and see mysql_priv.h for a list of the possible flags. Currently
 * (2007-10-04), the following flags are available:
 *
 * <pre>
 *     MODE_REAL_AS_FLOAT==0x1
 *     MODE_PIPES_AS_CONCAT==0x2
 *     MODE_ANSI_QUOTES==0x4
 *     MODE_IGNORE_SPACE==0x8
 *     MODE_NOT_USED==0x10
 *     MODE_ONLY_FULL_GROUP_BY==0x20
 *     MODE_NO_UNSIGNED_SUBTRACTION==0x40
 *     MODE_NO_DIR_IN_CREATE==0x80
 *     MODE_POSTGRESQL==0x100
 *     MODE_ORACLE==0x200
 *     MODE_MSSQL==0x400
 *     MODE_DB2==0x800
 *     MODE_MAXDB==0x1000
 *     MODE_NO_KEY_OPTIONS==0x2000
 *     MODE_NO_TABLE_OPTIONS==0x4000
 *     MODE_NO_FIELD_OPTIONS==0x8000
 *     MODE_MYSQL323==0x10000
 *     MODE_MYSQL323==0x20000
 *     MODE_MYSQL40==0x40000
 *     MODE_ANSI==0x80000
 *     MODE_NO_AUTO_VALUE_ON_ZERO==0x100000
 *     MODE_NO_BACKSLASH_ESCAPES==0x200000
 *     MODE_STRICT_TRANS_TABLES==0x400000
 *     MODE_STRICT_ALL_TABLES==0x800000
 *     MODE_NO_ZERO_IN_DATE==0x1000000
 *     MODE_NO_ZERO_DATE==0x2000000
 *     MODE_INVALID_DATES==0x4000000
 *     MODE_ERROR_FOR_DIVISION_BY_ZERO==0x8000000
 *     MODE_TRADITIONAL==0x10000000
 *     MODE_NO_AUTO_CREATE_USER==0x20000000
 *     MODE_HIGH_NOT_PRECEDENCE==0x40000000
 *     MODE_PAD_CHAR_TO_FULL_LENGTH==0x80000000
 * </pre>
 *
 * All these flags are replicated from the server. However, all flags except
 * MODE_NO_DIR_IN_CREATE are honored by the slave; the slave always preserves
 * its old value of MODE_NO_DIR_IN_CREATE. For a rationale, see comment in
 * Query_log_event::do_apply_event in log_event.cc. This field is always written
 * to the binlog.</td>
 * </tr>
 * <tr>
 * <td>catalog</td>
 * <td>Q_CATALOG_NZ_CODE == 6</td>
 * <td>Variable-length string: the length in bytes (1 byte) followed by the
 * characters (at most 255 bytes)</td>
 * <td>Stores the client's current catalog. Every database belongs to a catalog,
 * the same way that every table belongs to a database. Currently, there is only
 * one catalog, "std". This field is written if the length of the catalog is >
 * 0; otherwise it is not written.</td>
 * </tr>
 * <tr>
 * <td>auto_increment</td>
 * <td>Q_AUTO_INCREMENT == 3</td>
 * <td>two 2 byte unsigned integers, totally 2+2=4 bytes</td>
 * <td>The two variables auto_increment_increment and auto_increment_offset, in
 * that order. For more information, see "System variables" in the MySQL manual.
 * This field is written if auto_increment > 1. Otherwise, it is not written.</td>
 * </tr>
 * <tr>
 * <td>charset</td>
 * <td>Q_CHARSET_CODE == 4</td>
 * <td>three 2 byte unsigned integers, totally 2+2+2=6 bytes</td>
 * <td>The three variables character_set_client, collation_connection, and
 * collation_server, in that order. character_set_client is a code identifying
 * the character set and collation used by the client to encode the query.
 * collation_connection identifies the character set and collation that the
 * master converts the query to when it receives it; this is useful when
 * comparing literal strings. collation_server is the default character set and
 * collation used when a new database is created. See also
 * "Connection Character Sets and Collations" in the MySQL 5.1 manual. All three
 * variables are codes identifying a (character set, collation) pair. To see
 * which codes map to which pairs, run the query "SELECT id, character_set_name,
 * collation_name FROM COLLATIONS". Cf. Q_CHARSET_DATABASE_CODE below. This
 * field is always written.</td>
 * </tr>
 * <tr>
 * <td>time_zone</td>
 * <td>Q_TIME_ZONE_CODE == 5</td>
 * <td>Variable-length string: the length in bytes (1 byte) followed by the
 * characters (at most 255 bytes).
 * <td>The time_zone of the master. See also "System Variables" and
 * "MySQL Server Time Zone Support" in the MySQL manual. This field is written
 * if the length of the time zone string is > 0; otherwise, it is not written.</td>
 * </tr>
 * <tr>
 * <td>lc_time_names_number</td>
 * <td>Q_LC_TIME_NAMES_CODE == 7</td>
 * <td>2 byte integer</td>
 * <td>A code identifying a table of month and day names. The mapping from codes
 * to languages is defined in sql_locale.cc. This field is written if it is not
 * 0, i.e., if the locale is not en_US.</td>
 * </tr>
 * <tr>
 * <td>charset_database_number</td>
 * <td>Q_CHARSET_DATABASE_CODE == 8</td>
 * <td>2 byte integer</td>
 * <td>The value of the collation_database system variable (in the source code
 * stored in thd->variables.collation_database), which holds the code for a
 * (character set, collation) pair as described above (see Q_CHARSET_CODE).
 * collation_database was used in old versions (???WHEN). Its value was loaded
 * when issuing a "use db" query and could be changed by issuing a
 * "SET collation_database=xxx" query. It used to affect the "LOAD DATA INFILE"
 * and "CREATE TABLE" commands. In newer versions, "CREATE TABLE" has been
 * changed to take the character set from the database of the created table,
 * rather than the character set of the current database. This makes a
 * difference when creating a table in another database than the current one.
 * "LOAD DATA INFILE" has not yet changed to do this, but there are plans to
 * eventually do it, and to make collation_database read-only. This field is
 * written if it is not 0.</td>
 * </tr>
 * <tr>
 * <td>table_map_for_update</td>
 * <td>Q_TABLE_MAP_FOR_UPDATE_CODE == 9</td>
 * <td>8 byte integer</td>
 * <td>The value of the table map that is to be updated by the multi-table
 * update query statement. Every bit of this variable represents a table, and is
 * set to 1 if the corresponding table is to be updated by this statement. The
 * value of this variable is set when executing a multi-table update statement
 * and used by slave to apply filter rules without opening all the tables on
 * slave. This is required because some tables may not exist on slave because of
 * the filter rules.</td>
 * </tr>
 * </table>
 *
 * Query_log_event_notes_on_previous_versions Notes on Previous Versions Status vars were introduced
 * in version 5.0. To read earlier versions correctly, check the length of the Post-Header. The
 * status variable Q_CATALOG_CODE == 2 existed in MySQL 5.0.x, where 0<=x<=3. It was identical to
 * Q_CATALOG_CODE, except that the string had a trailing '\0'. The '\0' was removed in 5.0.4 since
 * it was redundant (the string length is stored before the string). The Q_CATALOG_CODE will never
 * be written by a new master, but can still be understood by a new slave. See
 * Q_CHARSET_DATABASE_CODE in the table above. When adding new status vars, please don't forget to
 * update the MAX_SIZE_LOG_EVENT_STATUS, and update function code_name
 *
 * @see mysql-5.1.6/sql/logevent.cc - Query_log_event
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public class QueryLogEvent extends LogEvent {

  /**
   * The maximum number of updated databases that a status of Query-log-event can carry. It can
   * redefined within a range [1.. OVER_MAX_DBS_IN_EVENT_MTS].
   */
  public static final int MAX_DBS_IN_EVENT_MTS = 16;

  /**
   * When the actual number of databases exceeds MAX_DBS_IN_EVENT_MTS the value of
   * OVER_MAX_DBS_IN_EVENT_MTS is is put into the mts_accessed_dbs status.
   */
  public static final int OVER_MAX_DBS_IN_EVENT_MTS = 254;

  public static final int SYSTEM_CHARSET_MBMAXLEN = 3;
  public static final int NAME_CHAR_LEN = 64;
  /* Field/table name length */
  public static final int NAME_LEN = (NAME_CHAR_LEN * SYSTEM_CHARSET_MBMAXLEN);

  /**
   * Max number of possible extra bytes in a replication event compared to a packet (i.e. a query)
   * sent from client to master; First, an auxiliary log_event status vars estimation:
   */
  public static final int MAX_SIZE_LOG_EVENT_STATUS =
      (1
              + 4 /* type, flags2 */
              + 1
              + 8 /*
                   * type,
                   * sql_mode
                   */
              + 1
              + 1
              + 255 /*
                     * type,
                     * length
                     * ,
                     * catalog
                     */
              + 1
              + 4 /*
                   * type,
                   * auto_increment
                   */
              + 1
              + 6 /*
                   * type,
                   * charset
                   */
              + 1
              + 1
              + 255 /*
                     * type,
                     * length
                     * ,
                     * time_zone
                     */
              + 1
              + 2 /*
                   * type,
                   * lc_time_names_number
                   */
              + 1
              + 2 /*
                   * type,
                   * charset_database_number
                   */
              + 1
              + 8 /*
                   * type,
                   * table_map_for_update
                   */
              + 1
              + 4 /*
                   * type,
                   * master_data_written
                   */
              /*
               * type, db_1, db_2,
               * ...
               */
              /* type, microseconds */
              /*
               * MariaDb type,
               * sec_part of NOW()
               */
              + 1
              + (MAX_DBS_IN_EVENT_MTS * (1 + NAME_LEN))
              + 3 /*
                   * type
                   * ,
                   * microseconds
                   */
              + 1
              + 32 * 3
              + 1
              + 60 /*
                    * type ,
                    * user_len
                    * , user ,
                    * host_len
                    * , host
                    */)
          + 1
          + 1 /*
               * type,
               * explicit_def
               * ..ts
               */
          + 1
          + 8 /*
               * type,
               * xid
               * of
               * DDL
               */
          + 1
          + 2 /*
               * type
               * ,
               * default_collation_for_utf8mb4_number
               */
          + 1 /* sql_require_primary_key */;
  /**
   * Fixed data part:
   *
   * <ul>
   *   <li>4 bytes. The ID of the thread that issued this statement. Needed for temporary tables.
   *       This is also useful for a DBA for knowing who did what on the master.
   *   <li>4 bytes. The time in seconds that the statement took to execute. Only useful for
   *       inspection by the DBA.
   *   <li>1 byte. The length of the name of the database which was the default database when the
   *       statement was executed. This name appears later, in the variable data part. It is
   *       necessary for statements such as INSERT INTO t VALUES(1) that don't specify the database
   *       and rely on the default database previously selected by USE.
   *   <li>2 bytes. The error code resulting from execution of the statement on the master. Error
   *       codes are defined in include/mysqld_error.h. 0 means no error. How come statements with a
   *       non-zero error code can exist in the binary log? This is mainly due to the use of
   *       non-transactional tables within transactions. For example, if an INSERT ... SELECT fails
   *       after inserting 1000 rows into a MyISAM table (for example, with a duplicate-key
   *       violation), we have to write this statement to the binary log, because it truly modified
   *       the MyISAM table. For transactional tables, there should be no event with a non-zero
   *       error code (though it can happen, for example if the connection was interrupted
   *       (Control-C)). The slave checks the error code: After executing the statement itself, it
   *       compares the error code it got with the error code in the event, and if they are
   *       different it stops replicating (unless --slave-skip-errors was used to ignore the error).
   *   <li>2 bytes (not present in v1, v3). The length of the status variable block.
   * </ul>
   *
   * Variable part:
   *
   * <ul>
   *   <li>Zero or more status variables (not present in v1, v3). Each status variable consists of
   *       one byte code identifying the variable stored, followed by the value of the variable. The
   *       format of the value is variable-specific, as described later.
   *   <li>The default database name (null-terminated).
   *   <li>The SQL statement. The slave knows the size of the other fields in the variable part (the
   *       sizes are given in the fixed data part), so by subtraction it can know the size of the
   *       statement.
   * </ul>
   *
   * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
   */
  private String user;

  private String host;

  /* using byte for query string */
  protected String query;
  protected String catalog;
  protected final String dbname;

  /** The number of seconds the query took to run on the master. */
  // The time in seconds that the statement took to execute. Only useful for
  // inspection by the DBA
  private final long execTime;

  private final int errorCode;
  private final long sessionId; /* thread_id */

  /**
   * 'flags2' is a second set of flags (on top of those in Log_event), for session variables. These
   * are thd->options which is & against a mask (OPTIONS_WRITTEN_TO_BIN_LOG).
   */
  private long flags2;

  /** In connections sql_mode is 32 bits now but will be 64 bits soon */
  private long sql_mode;

  private long autoIncrementIncrement = -1;
  private long autoIncrementOffset = -1;

  private int clientCharset = -1;
  private int clientCollation = -1;
  private int serverCollation = -1;
  private int tvSec = -1;
  private BigInteger ddlXid = BigInteger.valueOf(-1L);
  private String charsetName;

  private String timezone;

  public QueryLogEvent(
      LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent)
      throws IOException {
    super(header);

    final int commonHeaderLen = descriptionEvent.commonHeaderLen;
    final int postHeaderLen = descriptionEvent.postHeaderLen[header.type - 1];
    /*
     * We test if the event's length is sensible, and if so we compute
     * data_len. We cannot rely on QUERY_HEADER_LEN here as it would not be
     * format-tolerant. We use QUERY_HEADER_MINIMAL_LEN which is the same
     * for 3.23, 4.0 & 5.0.
     */
    if (buffer.limit() < (commonHeaderLen + postHeaderLen)) {
      throw new IOException("Query event length is too short.");
    }
    int dataLen = buffer.limit() - (commonHeaderLen + postHeaderLen);
    buffer.position(commonHeaderLen + Q_THREAD_ID_OFFSET);

    sessionId = buffer.getUint32(); // Q_THREAD_ID_OFFSET
    execTime = buffer.getUint32(); // Q_EXEC_TIME_OFFSET

    // TODO: add a check of all *_len vars
    final int dbLen = buffer.getUint8(); // Q_DB_LEN_OFFSET
    errorCode = buffer.getUint16(); // Q_ERR_CODE_OFFSET

    /*
     * 5.0 format starts here. Depending on the format, we may or not have
     * affected/warnings etc The remaining post-header to be parsed has
     * length:
     */
    int statusVarsLen = 0;
    if (postHeaderLen > QUERY_HEADER_MINIMAL_LEN) {
      statusVarsLen = buffer.getUint16(); // Q_STATUS_VARS_LEN_OFFSET
      /*
       * Check if status variable length is corrupt and will lead to very
       * wrong data. We could be even more strict and require data_len to
       * be even bigger, but this will suffice to catch most corruption
       * errors that can lead to a crash.
       */
      if (statusVarsLen > Math.min(dataLen, MAX_SIZE_LOG_EVENT_STATUS)) {
        throw new IOException(
            "status_vars_len (" + statusVarsLen + ") > data_len (" + dataLen + ")");
      }
      dataLen -= statusVarsLen;
    }
    /*
     * We have parsed everything we know in the post header for QUERY_EVENT,
     * the rest of post header is either comes from older version MySQL or
     * dedicated to derived events (e.g. Execute_load_query...)
     */

    /* variable-part: the status vars; only in MySQL 5.0 */
    final int start = commonHeaderLen + postHeaderLen;
    final int limit = buffer.limit(); /* for restore */
    final int end = start + statusVarsLen;
    buffer.position(start).limit(end);
    unpackVariables(buffer, end);
    buffer.position(end);
    buffer.limit(limit);

    /* A 2nd variable part; this is common to all versions */
    final int queryLen = dataLen - dbLen - 1;
    dbname = buffer.getFixString(dbLen + 1);
    if (clientCharset >= 0) {
      charsetName = CharsetConversion.getJavaCharset(clientCharset);

      if ((charsetName != null) && (Charset.isSupported(charsetName))) {
        query = buffer.getFixString(queryLen, charsetName);
      } else {
        logger.warn(
            "unsupported character set in query log: "
                + "\n    ID = "
                + clientCharset
                + ", Charset = "
                + CharsetConversion.getCharset(clientCharset)
                + ", Collation = "
                + CharsetConversion.getCollation(clientCharset));

        query = buffer.getFixString(queryLen);
      }
    } else {
      query = buffer.getFixString(queryLen);
    }
  }

  /* query event post-header */
  public static final int Q_THREAD_ID_OFFSET = 0;
  public static final int Q_EXEC_TIME_OFFSET = 4;
  public static final int Q_DB_LEN_OFFSET = 8;
  public static final int Q_ERR_CODE_OFFSET = 9;
  public static final int Q_STATUS_VARS_LEN_OFFSET = 11;
  public static final int Q_DATA_OFFSET = QUERY_HEADER_LEN;

  /* these are codes, not offsets; not more than 256 values (1 byte). */
  public static final int Q_FLAGS2_CODE = 0;
  public static final int Q_SQL_MODE_CODE = 1;

  /**
   * Q_CATALOG_CODE is catalog with end zero stored; it is used only by MySQL 5.0.x where 0<=x<=3.
   * We have to keep it to be able to replicate these old masters.
   */
  public static final int Q_CATALOG_CODE = 2;

  public static final int Q_AUTO_INCREMENT = 3;
  public static final int Q_CHARSET_CODE = 4;
  public static final int Q_TIME_ZONE_CODE = 5;

  /**
   * Q_CATALOG_NZ_CODE is catalog withOUT end zero stored; it is used by MySQL 5.0.x where x>=4.
   * Saves one byte in every Query_log_event in binlog, compared to Q_CATALOG_CODE. The reason we
   * didn't simply re-use Q_CATALOG_CODE is that then a 5.0.3 slave of this 5.0.x (x>=4) master
   * would crash (segfault etc) because it would expect a 0 when there is none.
   */
  public static final int Q_CATALOG_NZ_CODE = 6;

  public static final int Q_LC_TIME_NAMES_CODE = 7;

  public static final int Q_CHARSET_DATABASE_CODE = 8;

  public static final int Q_TABLE_MAP_FOR_UPDATE_CODE = 9;

  public static final int Q_MASTER_DATA_WRITTEN_CODE = 10;

  public static final int Q_INVOKER = 11;

  /**
   * Q_UPDATED_DB_NAMES status variable collects of the updated databases total number and their
   * names to be propagated to the slave in order to facilitate the parallel applying of the Query
   * events.
   */
  public static final int Q_UPDATED_DB_NAMES = 12;

  public static final int Q_MICROSECONDS = 13;
  /** A old (unused now) code for Query_log_event status similar to G_COMMIT_TS. */
  public static final int Q_COMMIT_TS = 14;
  /** A code for Query_log_event status, similar to G_COMMIT_TS2. */
  public static final int Q_COMMIT_TS2 = 15;
  /**
   * The master connection @@session.explicit_defaults_for_timestamp which is recorded for queries,
   * CREATE and ALTER table that is defined with a TIMESTAMP column, that are dependent on that
   * feature. For pre-WL6292 master's the associated with this code value is zero.
   */
  public static final int Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP = 16;

  /** The variable carries xid info of 2pc-aware (recoverable) DDL queries. */
  public static final int Q_DDL_LOGGED_WITH_XID = 17;
  /**
   * This variable stores the default collation for the utf8mb4 character set. Used to support
   * cross-version replication.
   */
  public static final int Q_DEFAULT_COLLATION_FOR_UTF8MB4 = 18;

  /** Replicate sql_require_primary_key. */
  public static final int Q_SQL_REQUIRE_PRIMARY_KEY = 19;

  /** FROM MariaDB 5.5.34 */
  public static final int Q_HRNOW = 128;

  private final void unpackVariables(LogBuffer buffer, final int end) throws IOException {
    int code = -1;
    try {
      while (buffer.position() < end) {
        switch (code = buffer.getUint8()) {
          case Q_FLAGS2_CODE:
            flags2 = buffer.getUint32();
            break;
          case Q_SQL_MODE_CODE:
            sql_mode = buffer.getLong64(); // QQ: Fix when sql_mode
            // is ulonglong
            break;
          case Q_CATALOG_NZ_CODE:
            catalog = buffer.getString();
            break;
          case Q_AUTO_INCREMENT:
            autoIncrementIncrement = buffer.getUint16();
            autoIncrementOffset = buffer.getUint16();
            break;
          case Q_CHARSET_CODE:
            // Charset: 6 byte character set flag.
            // 1-2 = character set client
            // 3-4 = collation client
            // 5-6 = collation server
            clientCharset = buffer.getUint16();
            clientCollation = buffer.getUint16();
            serverCollation = buffer.getUint16();
            break;
          case Q_TIME_ZONE_CODE:
            timezone = buffer.getString();
            break;
          case Q_CATALOG_CODE: /* for 5.0.x where 0<=x<=3 masters */
            final int len = buffer.getUint8();
            catalog = buffer.getFixString(len + 1);
            break;
          case Q_LC_TIME_NAMES_CODE:
            // lc_time_names_number = buffer.getUint16();
            buffer.forward(2);
            break;
          case Q_CHARSET_DATABASE_CODE:
            // charset_database_number = buffer.getUint16();
            buffer.forward(2);
            break;
          case Q_TABLE_MAP_FOR_UPDATE_CODE:
            // table_map_for_update = buffer.getUlong64();
            buffer.forward(8);
            break;
          case Q_MASTER_DATA_WRITTEN_CODE:
            // data_written = master_data_written =
            // buffer.getUint32();
            buffer.forward(4);
            break;
          case Q_INVOKER:
            user = buffer.getString();
            host = buffer.getString();
            break;
          case Q_MICROSECONDS:
            // when.tv_usec= uint3korr(pos);
            tvSec = buffer.getInt24();
            break;
          case Q_UPDATED_DB_NAMES:
            int mtsAccessedDbs = buffer.getUint8();
            /**
             * Notice, the following check is positive also in case of the master's
             * MAX_DBS_IN_EVENT_MTS > the slave's one and the event contains e.g the master's
             * MAX_DBS_IN_EVENT_MTS db:s.
             */
            if (mtsAccessedDbs > MAX_DBS_IN_EVENT_MTS) {
              mtsAccessedDbs = OVER_MAX_DBS_IN_EVENT_MTS;
              break;
            }
            String mtsAccessedDbNames[] = new String[mtsAccessedDbs];
            for (int i = 0; i < mtsAccessedDbs && buffer.position() < end; i++) {
              int length = end - buffer.position();
              mtsAccessedDbNames[i] = buffer.getFixString(length < NAME_LEN ? length : NAME_LEN);
            }
            break;
          case Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP:
            // thd->variables.explicit_defaults_for_timestamp
            buffer.forward(1);
            break;
          case Q_DDL_LOGGED_WITH_XID:
            ddlXid = buffer.getUlong64();
            break;
          case Q_DEFAULT_COLLATION_FOR_UTF8MB4:
            // int2store(start,
            // default_collation_for_utf8mb4_number);
            buffer.forward(2);
            break;
          case Q_SQL_REQUIRE_PRIMARY_KEY:
            // *start++ = thd->variables.sql_require_primary_key;
            buffer.forward(1);
            break;
          case Q_HRNOW:
            // int when_sec_part = buffer.getUint24();
            buffer.forward(3);
            break;
          default:
            /*
             * That's why you must write status vars in growing
             * order of code
             */
            logger.error(
                "Query_log_event has unknown status vars (first has code: "
                    + code
                    + "), skipping the rest of them");
            break; // Break loop
        }
      }
    } catch (RuntimeException e) {
      throw new IOException("Read " + findCodeName(code) + " error: " + e.getMessage(), e);
    }
  }

  private static final String findCodeName(final int code) {
    switch (code) {
      case Q_FLAGS2_CODE:
        return "Q_FLAGS2_CODE";
      case Q_SQL_MODE_CODE:
        return "Q_SQL_MODE_CODE";
      case Q_CATALOG_CODE:
        return "Q_CATALOG_CODE";
      case Q_AUTO_INCREMENT:
        return "Q_AUTO_INCREMENT";
      case Q_CHARSET_CODE:
        return "Q_CHARSET_CODE";
      case Q_TIME_ZONE_CODE:
        return "Q_TIME_ZONE_CODE";
      case Q_CATALOG_NZ_CODE:
        return "Q_CATALOG_NZ_CODE";
      case Q_LC_TIME_NAMES_CODE:
        return "Q_LC_TIME_NAMES_CODE";
      case Q_CHARSET_DATABASE_CODE:
        return "Q_CHARSET_DATABASE_CODE";
      case Q_TABLE_MAP_FOR_UPDATE_CODE:
        return "Q_TABLE_MAP_FOR_UPDATE_CODE";
      case Q_MASTER_DATA_WRITTEN_CODE:
        return "Q_MASTER_DATA_WRITTEN_CODE";
      case Q_UPDATED_DB_NAMES:
        return "Q_UPDATED_DB_NAMES";
      case Q_MICROSECONDS:
        return "Q_MICROSECONDS";
      case Q_DDL_LOGGED_WITH_XID:
        return "Q_DDL_LOGGED_WITH_XID";
      case Q_DEFAULT_COLLATION_FOR_UTF8MB4:
        return "Q_DEFAULT_COLLATION_FOR_UTF8MB4";
      case Q_SQL_REQUIRE_PRIMARY_KEY:
        return "Q_SQL_REQUIRE_PRIMARY_KEY";
    }
    return "CODE#" + code;
  }

  public final String getUser() {
    return user;
  }

  public final String getHost() {
    return host;
  }

  public final String getQuery() {
    return query;
  }

  public final String getCatalog() {
    return catalog;
  }

  public final String getDbName() {
    return dbname;
  }

  /** The number of seconds the query took to run on the master. */
  public final long getExecTime() {
    return execTime;
  }

  public final int getErrorCode() {
    return errorCode;
  }

  public final long getSessionId() {
    return sessionId;
  }

  public final long getAutoIncrementIncrement() {
    return autoIncrementIncrement;
  }

  public final long getAutoIncrementOffset() {
    return autoIncrementOffset;
  }

  public final String getCharsetName() {
    return charsetName;
  }

  public final String getTimezone() {
    return timezone;
  }

  /**
   * Returns the charsetID value.
   *
   * @return Returns the charsetID.
   */
  public final int getClientCharset() {
    return clientCharset;
  }

  /**
   * Returns the clientCollationId value.
   *
   * @return Returns the clientCollationId.
   */
  public final int getClientCollation() {
    return clientCollation;
  }

  /**
   * Returns the serverCollationId value.
   *
   * @return Returns the serverCollationId.
   */
  public final int getServerCollation() {
    return serverCollation;
  }

  public int getTvSec() {
    return tvSec;
  }

  public BigInteger getDdlXid() {
    return ddlXid;
  }

  /**
   * Returns the sql_mode value.
   *
   * <p>The sql_mode variable. See the section "SQL Modes" in the MySQL manual, and see mysql_priv.h
   * for a list of the possible flags. Currently (2007-10-04), the following flags are available:
   *
   * <ul>
   *   <li>MODE_REAL_AS_FLOAT==0x1
   *   <li>MODE_PIPES_AS_CONCAT==0x2
   *   <li>MODE_ANSI_QUOTES==0x4
   *   <li>MODE_IGNORE_SPACE==0x8
   *   <li>MODE_NOT_USED==0x10
   *   <li>MODE_ONLY_FULL_GROUP_BY==0x20
   *   <li>MODE_NO_UNSIGNED_SUBTRACTION==0x40
   *   <li>MODE_NO_DIR_IN_CREATE==0x80
   *   <li>MODE_POSTGRESQL==0x100
   *   <li>MODE_ORACLE==0x200
   *   <li>MODE_MSSQL==0x400
   *   <li>MODE_DB2==0x800
   *   <li>MODE_MAXDB==0x1000
   *   <li>MODE_NO_KEY_OPTIONS==0x2000
   *   <li>MODE_NO_TABLE_OPTIONS==0x4000
   *   <li>MODE_NO_FIELD_OPTIONS==0x8000
   *   <li>MODE_MYSQL323==0x10000
   *   <li>MODE_MYSQL40==0x20000
   *   <li>MODE_ANSI==0x40000
   *   <li>MODE_NO_AUTO_VALUE_ON_ZERO==0x80000
   *   <li>MODE_NO_BACKSLASH_ESCAPES==0x100000
   *   <li>MODE_STRICT_TRANS_TABLES==0x200000
   *   <li>MODE_STRICT_ALL_TABLES==0x400000
   *   <li>MODE_NO_ZERO_IN_DATE==0x800000
   *   <li>MODE_NO_ZERO_DATE==0x1000000
   *   <li>MODE_INVALID_DATES==0x2000000
   *   <li>MODE_ERROR_FOR_DIVISION_BY_ZERO==0x4000000
   *   <li>MODE_TRADITIONAL==0x8000000
   *   <li>MODE_NO_AUTO_CREATE_USER==0x10000000
   *   <li>MODE_HIGH_NOT_PRECEDENCE==0x20000000
   *   <li>MODE_NO_ENGINE_SUBSTITUTION=0x40000000
   *   <li>MODE_PAD_CHAR_TO_FULL_LENGTH==0x80000000
   * </ul>
   *
   * All these flags are replicated from the server. However, all flags except MODE_NO_DIR_IN_CREATE
   * are honored by the slave; the slave always preserves its old value of MODE_NO_DIR_IN_CREATE.
   * This field is always written to the binlog.
   */
  public final long getSqlMode() {
    return sql_mode;
  }

  /* FLAGS2 values that can be represented inside the binlog */
  public static final int OPTION_AUTO_IS_NULL = 1 << 14;
  public static final int OPTION_NOT_AUTOCOMMIT = 1 << 19;
  public static final int OPTION_NO_FOREIGN_KEY_CHECKS = 1 << 26;
  public static final int OPTION_RELAXED_UNIQUE_CHECKS = 1 << 27;

  /**
   * The flags in thd->options, binary AND-ed with OPTIONS_WRITTEN_TO_BIN_LOG. The thd->options
   * bitfield contains options for "SELECT". OPTIONS_WRITTEN identifies those options that need to
   * be written to the binlog (not all do). Specifically, OPTIONS_WRITTEN_TO_BIN_LOG equals
   * (OPTION_AUTO_IS_NULL | OPTION_NO_FOREIGN_KEY_CHECKS | OPTION_RELAXED_UNIQUE_CHECKS |
   * OPTION_NOT_AUTOCOMMIT), or 0x0c084000 in hex. These flags correspond to the SQL variables
   * SQL_AUTO_IS_NULL, FOREIGN_KEY_CHECKS, UNIQUE_CHECKS, and AUTOCOMMIT, documented in the "SET
   * Syntax" section of the MySQL Manual. This field is always written to the binlog in version >=
   * 5.0, and never written in version < 5.0.
   */
  public final long getFlags2() {
    return flags2;
  }

  /** Returns the OPTION_AUTO_IS_NULL flag. */
  public final boolean isAutoIsNull() {
    return ((flags2 & OPTION_AUTO_IS_NULL) == OPTION_AUTO_IS_NULL);
  }

  /** Returns the OPTION_NO_FOREIGN_KEY_CHECKS flag. */
  public final boolean isForeignKeyChecks() {
    return ((flags2 & OPTION_NO_FOREIGN_KEY_CHECKS) != OPTION_NO_FOREIGN_KEY_CHECKS);
  }

  /** Returns the OPTION_NOT_AUTOCOMMIT flag. */
  public final boolean isAutocommit() {
    return ((flags2 & OPTION_NOT_AUTOCOMMIT) != OPTION_NOT_AUTOCOMMIT);
  }

  /** Returns the OPTION_NO_FOREIGN_KEY_CHECKS flag. */
  public final boolean isUniqueChecks() {
    return ((flags2 & OPTION_RELAXED_UNIQUE_CHECKS) != OPTION_RELAXED_UNIQUE_CHECKS);
  }
}
