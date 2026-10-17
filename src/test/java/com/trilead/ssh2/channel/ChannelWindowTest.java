package com.trilead.ssh2.channel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.trilead.ssh2.Connection;
import com.trilead.ssh2.packets.PacketChannelOpenConfirmation;
import com.trilead.ssh2.packets.Packets;
import com.trilead.ssh2.packets.TypesReader;
import com.trilead.ssh2.packets.TypesWriter;
import com.trilead.ssh2.transport.TransportManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests that channels advertise, accept and refill the configured receive window.
 */
public class ChannelWindowTest
{
	private static final int REMOTE_ID = 7;
	private static final int PACKET_SIZE = 32768;
	private static final int LARGE_WINDOW = 1024 * 1024;
	private static final int ROUNDS = 4;

	private final List<byte[]> sentMessages = new CopyOnWriteArrayList<>();
	private TransportManager tm;
	private ChannelManager cm;
	private int advertisedWindow;

	@Before
	public void setUp() throws IOException
	{
		tm = mock(TransportManager.class);
		doAnswer(invocation -> {
			byte[] msg = ((byte[]) invocation.getArgument(0)).clone();
			sentMessages.add(msg);
			if (msg[0] == Packets.SSH_MSG_CHANNEL_OPEN)
				confirmOpen(msg);
			return null;
		}).when(tm).sendMessage(any(byte[].class));
	}

	@Test
	public void defaultWindowIsUnchanged() throws IOException
	{
		cm = new ChannelManager(tm);
		openChannel();
		assertEquals(Channel.CHANNEL_BUFFER_SIZE, advertisedWindow);
	}

	@Test
	public void channelsAdvertiseConfiguredWindow() throws IOException
	{
		cm = new ChannelManager(tm, LARGE_WINDOW);
		openChannel();
		assertEquals(LARGE_WINDOW, advertisedWindow);
	}

	@Test
	public void windowSetAfterCreationAppliesToNewChannels() throws IOException
	{
		cm = new ChannelManager(tm);
		cm.setChannelWindowSize(LARGE_WINDOW);
		openChannel();
		assertEquals(LARGE_WINDOW, advertisedWindow);
	}

	@Test
	public void serverCanKeepStreamingWithAtLeastHalfTheWindow() throws IOException
	{
		cm = new ChannelManager(tm, LARGE_WINDOW);
		Channel c = openChannel();

		int streamed = 0;
		for (int round = 0; round < ROUNDS; round++)
		{
			int credit = c.localWindow;
			sendData(c, credit);
			readAll(c, credit);
			streamed += credit;
			assertTrue("credit after round " + round + ": " + c.localWindow, c.localWindow >= LARGE_WINDOW / 2);
		}

		assertEquals(streamed - LARGE_WINDOW + c.localWindow, sumOfWindowAdjustments());
	}

	@Test(expected = IOException.class)
	public void dataBeyondWindowIsRejected() throws IOException
	{
		cm = new ChannelManager(tm, LARGE_WINDOW);
		Channel c = openChannel();

		sendData(c, LARGE_WINDOW + 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void nonPositiveWindowIsRejected()
	{
		new Connection("localhost").setChannelWindowSize(0);
	}

	private Channel openChannel() throws IOException
	{
		return cm.openDirectTCPIPChannel("localhost", 3389, "127.0.0.1", 50000);
	}

	/** Answers a channel open the way a server would, so the opening call returns. */
	private void confirmOpen(byte[] openMsg) throws IOException
	{
		TypesReader tr = new TypesReader(openMsg);
		tr.readByte();
		tr.readString();
		int localId = tr.readUINT32();
		advertisedWindow = tr.readUINT32();

		byte[] confirmation = new PacketChannelOpenConfirmation(localId, REMOTE_ID, LARGE_WINDOW, PACKET_SIZE)
				.getPayload();
		cm.handleMessage(confirmation, confirmation.length);
	}

	private void sendData(Channel c, int total) throws IOException
	{
		byte[] data = new byte[PACKET_SIZE];
		for (int sent = 0; sent < total;)
		{
			int len = Math.min(PACKET_SIZE, total - sent);
			TypesWriter tw = new TypesWriter();
			tw.writeByte(Packets.SSH_MSG_CHANNEL_DATA);
			tw.writeUINT32(c.localID);
			tw.writeString(data, 0, len);
			byte[] msg = tw.getBytes();
			cm.handleMessage(msg, msg.length);
			sent += len;
		}
	}

	private void readAll(Channel c, int total) throws IOException
	{
		InputStream in = c.getStdoutStream();
		byte[] buffer = new byte[Channel.CHANNEL_BUFFER_SIZE];
		for (int read = 0; read < total;)
			read += in.read(buffer);
	}

	private int sumOfWindowAdjustments() throws IOException
	{
		int sum = 0;
		for (byte[] msg : sentMessages)
		{
			if (msg[0] != Packets.SSH_MSG_CHANNEL_WINDOW_ADJUST)
				continue;
			TypesReader tr = new TypesReader(msg);
			tr.readByte();
			tr.readUINT32();
			sum += tr.readUINT32();
		}
		return sum;
	}
}
