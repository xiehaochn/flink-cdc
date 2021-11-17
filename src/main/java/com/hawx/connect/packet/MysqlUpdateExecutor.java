package com.hawx.connect.packet;

import com.hawx.connect.MysqlConnector;
import com.hawx.connect.packet.client.QueryCommandPacket;
import com.hawx.connect.packet.server.ErrorPacket;
import com.hawx.connect.packet.server.OKPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 默认输出的数据编码为UTF-8，如有需要请正确转码
 *
 * @author jianghang 2013-9-4 上午11:51:11
 * @since 1.0.0
 */
public class MysqlUpdateExecutor {

  private static final Logger logger = LoggerFactory.getLogger(MysqlUpdateExecutor.class);

  private MysqlConnector connector;

  public MysqlUpdateExecutor(MysqlConnector connector) throws IOException {
    if (!connector.isConnected()) {
      throw new IOException("should execute connector.connect() first");
    }

    this.connector = connector;
  }

  /*
   * public MysqlUpdateExecutor(SocketChannel ch){ this.channel = ch; }
   */

  public OKPacket update(String updateString) throws IOException {
    QueryCommandPacket cmd = new QueryCommandPacket();
    cmd.setQueryString(updateString);
    byte[] bodyBytes = cmd.toBytes();
    PacketManager.writeBody(connector.getChannel(), bodyBytes);

    logger.debug("read update result...");
    byte[] body =
        PacketManager.readBytes(
            connector.getChannel(),
            PacketManager.readHeader(connector.getChannel(), 4).getPacketBodyLength());
    if (body[0] < 0) {
      ErrorPacket packet = new ErrorPacket();
      packet.fromBytes(body);
      throw new IOException(packet + "\n with command: " + updateString);
    }

    OKPacket packet = new OKPacket();
    packet.fromBytes(body);
    return packet;
  }
}
