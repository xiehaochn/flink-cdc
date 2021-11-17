package com.hawx.connect.socket;

import java.io.IOException;
import java.net.SocketAddress;

public interface SocketChannel {

  public void write(byte[]... buf) throws IOException;

  public byte[] read(int readSize) throws IOException;

  public byte[] read(int readSize, int timeout) throws IOException;

  public void read(byte[] data, int off, int len, int timeout) throws IOException;

  public boolean isConnected();

  public SocketAddress getRemoteSocketAddress();

  public SocketAddress getLocalSocketAddress();

  public void close();
}
