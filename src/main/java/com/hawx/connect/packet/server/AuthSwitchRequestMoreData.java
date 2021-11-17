package com.hawx.connect.packet.server;

import com.hawx.connect.packet.ByteHelper;
import com.hawx.connect.packet.CommandPacket;

import java.io.IOException;

public class AuthSwitchRequestMoreData extends CommandPacket {

  public int status;
  public byte[] authData;

  public void fromBytes(byte[] data) {
    int index = 0;
    // 1. read status
    status = data[index];
    index += 1;
    authData = ByteHelper.readNullTerminatedBytes(data, index);
  }

  public byte[] toBytes() throws IOException {
    return null;
  }
}
