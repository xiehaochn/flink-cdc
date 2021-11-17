package com.hawx.connect.packet;

import com.hawx.utils.BinlogToStringStyle;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.flink.shaded.curator4.com.google.common.base.Preconditions;

public abstract class PacketWithHeaderPacket implements IPacket {

  protected HeaderPacket header;

  protected PacketWithHeaderPacket() {}

  protected PacketWithHeaderPacket(HeaderPacket header) {
    setHeader(header);
  }

  public void setHeader(HeaderPacket header) {
    Preconditions.checkNotNull(header);
    this.header = header;
  }

  public HeaderPacket getHeader() {
    return header;
  }

  public String toString() {
    return ToStringBuilder.reflectionToString(this, new BinlogToStringStyle());
  }
}
