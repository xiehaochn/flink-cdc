package com.hawx.connect.packet;

import com.hawx.utils.BinlogToStringStyle;
import org.apache.commons.lang3.builder.ToStringBuilder;

public abstract class CommandPacket implements IPacket {

  private byte command;

  // arg

  public void setCommand(byte command) {
    this.command = command;
  }

  public byte getCommand() {
    return command;
  }

  public String toString() {
    return ToStringBuilder.reflectionToString(this, new BinlogToStringStyle());
  }
}
