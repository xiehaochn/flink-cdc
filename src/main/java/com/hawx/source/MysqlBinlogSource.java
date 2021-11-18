package com.hawx.source;

import com.hawx.connect.MysqlConnector;
import com.hawx.connect.packet.HeaderPacket;
import com.hawx.connect.packet.MysqlUpdateExecutor;
import com.hawx.connect.packet.PacketManager;
import com.hawx.connect.packet.client.BinlogDumpCommandPacket;
import com.hawx.connect.packet.client.RegisterSlaveCommandPacket;
import com.hawx.connect.packet.client.SemiAckCommandPacket;
import com.hawx.connect.packet.server.ErrorPacket;
import com.hawx.entity.DataBase;
import com.hawx.entity.event.LogContext;
import com.hawx.entity.event.LogDecoder;
import com.hawx.entity.event.LogEvent;
import com.hawx.entity.event.innodb.FormatDescriptionLogEvent;
import com.hawx.utils.DirectLogFetcher;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MysqlBinlogSource extends RichParallelSourceFunction<LogEvent> {
  private static final String MARIA_SLAVE_CAPABILITY_MINE = "4";
  private static final long MASTER_HEARTBEAT_PERIOD_SECONDS = 15;
  private static final long BINLOG_START_OFFSET = 4L;
  public static final int BINLOG_CHECKSUM_ALG_OFF = 0;

  private Logger logger = LoggerFactory.getLogger(MysqlBinlogSource.class);
  private int defaultConnectionTimeoutInSeconds = 30; // sotimeout
  private int receiveBufferSize = 64 * 1024;
  private int sendBufferSize = 64 * 1024;
  protected final AtomicLong receivedBinlogBytes = new AtomicLong(0L);
  private String destination = "tmpDestination"; // 队列名字
  private DataBase dataBase =
      new DataBase(
          "mysql",
          "42.192.41.149",
          3306,
          "jdbc:mysql://42.192.41.149:3306",
          "hawx",
          "xxx147258",
          "hawx");

  private long slaveId;
  private MysqlConnector mysqlConnector;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    initBinlogConnector(dataBase);
    mysqlConnector.setReceiveBufferSize(receiveBufferSize);
    mysqlConnector.setSendBufferSize(sendBufferSize);
    mysqlConnector.setSoTimeout(defaultConnectionTimeoutInSeconds * 1000);
    //    mysqlConnector.setCharset(connectionCharset);
    //    mysqlConnector.setReceivedBinlogBytes(receivedBinlogBytes);
    // 随机生成slaveId
    if (this.slaveId <= 0) {
      this.slaveId = generateUniqueServerId();
    }
    mysqlConnector.connect();
    updateSettings();
    sendRegisterSlave();
    sendBinlogDump("mysql-bin.000001", BINLOG_START_OFFSET);
  }

  private void accumulateReceivedBytes(long x) {
    if (receivedBinlogBytes != null) {
      receivedBinlogBytes.addAndGet(x);
    }
  }

  private void sendRegisterSlave() throws IOException {
    RegisterSlaveCommandPacket cmd = new RegisterSlaveCommandPacket();
    SocketAddress socketAddress = mysqlConnector.getChannel().getLocalSocketAddress();
    if (socketAddress == null || !(socketAddress instanceof InetSocketAddress)) {
      return;
    }

    InetSocketAddress address = (InetSocketAddress) socketAddress;
    String host = address.getHostString();
    int port = address.getPort();
    cmd.reportHost = host;
    cmd.reportPort = port;
    cmd.reportPasswd = dataBase.getPassword();
    cmd.reportUser = dataBase.getUser();
    cmd.serverId = this.slaveId;
    byte[] cmdBody = cmd.toBytes();

    logger.info("Register slave {}", cmd);

    HeaderPacket header = new HeaderPacket();
    header.setPacketBodyLength(cmdBody.length);
    header.setPacketSequenceNumber((byte) 0x00);
    PacketManager.writePkg(mysqlConnector.getChannel(), header.toBytes(), cmdBody);

    header = PacketManager.readHeader(mysqlConnector.getChannel(), 4);
    byte[] body =
        PacketManager.readBytes(mysqlConnector.getChannel(), header.getPacketBodyLength());
    assert body != null;
    if (body[0] < 0) {
      if (body[0] == -1) {
        ErrorPacket err = new ErrorPacket();
        err.fromBytes(body);
        throw new IOException("Error When doing Register slave:" + err.toString());
      } else {
        throw new IOException("unpexpected packet with field_count=" + body[0]);
      }
    }
  }

  private void sendBinlogDump(String binlogFileName, Long binlogPosition) throws IOException {
    BinlogDumpCommandPacket binlogDumpCmd = new BinlogDumpCommandPacket();
    binlogDumpCmd.binlogFileName = binlogFileName;
    binlogDumpCmd.binlogPosition = binlogPosition;
    binlogDumpCmd.slaveServerId = this.slaveId;
    byte[] cmdBody = binlogDumpCmd.toBytes();

    logger.info("COM_BINLOG_DUMP with position:{}", binlogDumpCmd);
    HeaderPacket binlogDumpHeader = new HeaderPacket();
    binlogDumpHeader.setPacketBodyLength(cmdBody.length);
    binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
    PacketManager.writePkg(mysqlConnector.getChannel(), binlogDumpHeader.toBytes(), cmdBody);
    mysqlConnector.setDumping(true);
  }

  private void updateSettings() throws IOException {
    try {
      update("set wait_timeout=9999999");
    } catch (Exception e) {
      logger.warn("update wait_timeout failed", e);
    }
    try {
      update("set net_write_timeout=7200");
    } catch (Exception e) {
      logger.warn("update net_write_timeout failed", e);
    }

    try {
      update("set net_read_timeout=7200");
    } catch (Exception e) {
      logger.warn("update net_read_timeout failed", e);
    }

    try {
      // 设置服务端返回结果时不做编码转化，直接按照数据库的二进制编码进行发送，由客户端自己根据需求进行编码转化
      update("set names 'binary'");
    } catch (Exception e) {
      logger.warn("update names failed", e);
    }

    try {
      // mysql5.6针对checksum支持需要设置session变量
      // 如果不设置会出现错误： Slave can not handle replication events with the
      // checksum that master is configured to log
      // 但也不能乱设置，需要和mysql server的checksum配置一致，不然RotateLogEvent会出现乱码
      // '@@global.binlog_checksum'需要去掉单引号,在mysql 5.6.29下导致master退出
      update("set @master_binlog_checksum= @@global.binlog_checksum");
    } catch (Exception e) {
      if (!StringUtils.contains(e.getMessage(), "Unknown system variable")) {
        logger.warn("update master_binlog_checksum failed", e);
      }
    }

    try {
      // 参考:https://github.com/alibaba/canal/issues/284
      // mysql5.6需要设置slave_uuid避免被server kill链接
      update("set @slave_uuid=uuid()");
    } catch (Exception e) {
      if (!StringUtils.contains(e.getMessage(), "Unknown system variable")) {
        logger.warn("update slave_uuid failed", e);
      }
    }

    try {
      // mariadb针对特殊的类型，需要设置session变量
      update("SET @mariadb_slave_capability='" + MARIA_SLAVE_CAPABILITY_MINE + "'");
    } catch (Exception e) {
      if (!StringUtils.contains(e.getMessage(), "Unknown system variable")) {
        logger.warn("update mariadb_slave_capability failed", e);
      }
    }

    /**
     * MASTER_HEARTBEAT_PERIOD sets the interval in seconds between replication heartbeats. Whenever
     * the master's binary log is updated with an event, the waiting period for the next heartbeat
     * is reset. interval is a decimal value having the range 0 to 4294967 seconds and a resolution
     * in milliseconds; the smallest nonzero value is 0.001. Heartbeats are sent by the master only
     * if there are no unsent events in the binary log file for a period longer than interval.
     */
    try {
      long periodNano = TimeUnit.SECONDS.toNanos(MASTER_HEARTBEAT_PERIOD_SECONDS);
      update("SET @master_heartbeat_period=" + periodNano);
    } catch (Exception e) {
      logger.warn("update master_heartbeat_period failed", e);
    }
  }

  private final long generateUniqueServerId() throws UnknownHostException {
    // a=`echo $masterip|cut -d\. -f1`
    // b=`echo $masterip|cut -d\. -f2`
    // c=`echo $masterip|cut -d\. -f3`
    // d=`echo $masterip|cut -d\. -f4`
    // #server_id=`expr $a \* 256 \* 256 \* 256 + $b \* 256 \* 256 + $c
    // \* 256 + $d `
    // #server_id=$b$c$d
    // server_id=`expr $b \* 256 \* 256 + $c \* 256 + $d `
    InetAddress localHost = InetAddress.getLocalHost();
    byte[] addr = localHost.getAddress();
    int salt = (destination != null) ? destination.hashCode() : 0;
    return ((0x7f & salt) << 24)
        + ((0xff & (int) addr[1]) << 16) // NL
        + ((0xff & (int) addr[2]) << 8) // NL
        + (0xff & (int) addr[3]);
  }

  public void update(String cmd) throws IOException {
    MysqlUpdateExecutor exector = new MysqlUpdateExecutor(mysqlConnector);
    exector.update(cmd);
  }

  private void initBinlogConnector(DataBase dataBase) {
    mysqlConnector = new MysqlConnector();
    mysqlConnector.setAddress(new InetSocketAddress(dataBase.getHost(), dataBase.getPort()));
  }

  @Override
  public void close() throws Exception {
    super.close();
  }

  @Override
  public void run(SourceContext sourceContext) throws Exception {
    DirectLogFetcher fetcher = new DirectLogFetcher(mysqlConnector.getReceiveBufferSize());
    fetcher.start(mysqlConnector.getChannel());
    LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
    LogContext context = new LogContext();
    context.setFormatDescription(new FormatDescriptionLogEvent(4, BINLOG_CHECKSUM_ALG_OFF));
    while (fetcher.fetch()) {
      accumulateReceivedBytes(fetcher.limit());
      LogEvent event = null;
      event = decoder.decode(fetcher, context);

      if (event == null) {
        throw new Exception("parse failed");
      }
      // 处理binlog Event

      if (event.getSemival() == 1) {
        sendSemiAck(context.getLogPosition().getFileName(), context.getLogPosition().getPosition());
      }
      sourceContext.collect(event);
    }
  }

  public void sendSemiAck(String binlogfilename, Long binlogPosition) throws IOException {
    SemiAckCommandPacket semiAckCmd = new SemiAckCommandPacket();
    semiAckCmd.binlogFileName = binlogfilename;
    semiAckCmd.binlogPosition = binlogPosition;

    byte[] cmdBody = semiAckCmd.toBytes();

    logger.info("SEMI ACK with position:{}", semiAckCmd);
    HeaderPacket semiAckHeader = new HeaderPacket();
    semiAckHeader.setPacketBodyLength(cmdBody.length);
    semiAckHeader.setPacketSequenceNumber((byte) 0x00);
    PacketManager.writePkg(mysqlConnector.getChannel(), semiAckHeader.toBytes(), cmdBody);
  }

  @Override
  public void cancel() {}
}
