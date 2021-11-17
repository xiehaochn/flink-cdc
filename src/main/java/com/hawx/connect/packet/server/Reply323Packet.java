package com.hawx.connect.packet.server;

import com.hawx.connect.packet.ByteHelper;
import com.hawx.connect.packet.PacketWithHeaderPacket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Reply323Packet extends PacketWithHeaderPacket {

  public byte[] seed;

  public void fromBytes(byte[] data) throws IOException {}

  public byte[] toBytes() throws IOException {
    if (seed == null) {
      return new byte[] {(byte) 0};
    } else {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ByteHelper.writeNullTerminated(seed, out);
      return out.toByteArray();
    }
  }
}
