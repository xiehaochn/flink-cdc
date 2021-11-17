package com.hawx.connect.packet.server;

import com.hawx.connect.packet.ByteHelper;
import com.hawx.connect.packet.CommandPacket;

import java.io.IOException;

public class AuthSwitchRequestPacket extends CommandPacket {

  public int status;
  public String authName;
  public byte[] authData;

  public void fromBytes(byte[] data) {
    int index = 0;
    // 1. read status
    status = data[index];
    index += 1;
    byte[] authName = ByteHelper.readNullTerminatedBytes(data, index);
    this.authName = new String(authName);
    index += authName.length + 1;
    authData = ByteHelper.readNullTerminatedBytes(data, index);
  }

  public byte[] toBytes() throws IOException {
    return null;
  }
}
