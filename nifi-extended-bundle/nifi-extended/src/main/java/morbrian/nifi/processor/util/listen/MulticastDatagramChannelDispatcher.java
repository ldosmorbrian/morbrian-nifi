package morbrian.nifi.processor.util.listen;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.DatagramChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processor.util.listen.event.EventQueue;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;


public class MulticastDatagramChannelDispatcher<E extends Event<DatagramChannel>> extends DatagramChannelDispatcher {

    private final EventFactory<E> eventFactory;
    private final BlockingQueue<ByteBuffer> bufferPool;
    private final EventQueue<E> events;
    private final ComponentLog logger;
    private final String multicastGroup;
    private final String networkInterfaceName;

    private Selector selector;
    private DatagramChannel datagramChannel;
    private volatile boolean stopped = false;

    public MulticastDatagramChannelDispatcher(final EventFactory<E> eventFactory,
                                     final BlockingQueue<ByteBuffer> bufferPool,
                                     final BlockingQueue<E> events,
                                     final ComponentLog logger,
                                     final String networkInterfaceName,
                                     final String multicastGroup) {
        super(eventFactory, bufferPool, events, logger, multicastGroup, 0);
        this.eventFactory = eventFactory;
        this.bufferPool = bufferPool;
        this.logger = logger;
        this.multicastGroup = multicastGroup;
        this.events = new EventQueue<>(events, logger);
        this.networkInterfaceName = networkInterfaceName;

        if (bufferPool == null || bufferPool.size() == 0) {
            throw new IllegalArgumentException("A pool of available ByteBuffers is required");
        }
    }

    /**
     * Binds to the multicast group address and joins the multicast group.
     * @param nicAddress inherited but ignored for multicast implementation
     * @param port used to bind the multicast listen port
     * @param maxBufferSize @see DatagramChannelDispatcher#open
     * @throws IOException
     */
    @Override
    public void open(final InetAddress nicAddress, final int port, final int maxBufferSize) throws IOException {
        stopped = false;
        datagramChannel = DatagramChannel.open();
        datagramChannel.configureBlocking(false);

        if (maxBufferSize > 0) {
            datagramChannel.setOption(StandardSocketOptions.SO_RCVBUF, maxBufferSize);
            final int actualReceiveBufSize = datagramChannel.getOption(StandardSocketOptions.SO_RCVBUF);
            if (actualReceiveBufSize < maxBufferSize) {
                logger.warn("Attempted to set Socket Buffer Size to " + maxBufferSize + " bytes but could only set to "
                        + actualReceiveBufSize + "bytes. You may want to consider changing the Operating System's "
                        + "maximum receive buffer");
            }
        }

        // we don't have to worry about nicAddress being null here because InetSocketAddress already handles it
        datagramChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        datagramChannel.socket().bind(new InetSocketAddress(multicastGroup, port));

        // join the multicast group
        datagramChannel.join(InetAddress.getByName(multicastGroup), NetworkInterface.getByName(networkInterfaceName));

        selector = Selector.open();
        datagramChannel.register(selector, SelectionKey.OP_READ);
    }

    @Override
    public void run() {
        final ByteBuffer buffer = bufferPool.poll();
        while (!stopped) {
            try {
                int selected = selector.select();
                // if stopped the selector could already be closed which would result in a ClosedSelectorException
                if (selected > 0 && !stopped) {
                    Iterator<SelectionKey> selectorKeys = selector.selectedKeys().iterator();
                    // if stopped we don't want to modify the keys because close() may still be in progress
                    while (selectorKeys.hasNext() && !stopped) {
                        SelectionKey key = selectorKeys.next();
                        selectorKeys.remove();
                        if (!key.isValid()) {
                            continue;
                        }
                        DatagramChannel channel = (DatagramChannel) key.channel();
                        SocketAddress socketAddress;
                        buffer.clear();
                        while (!stopped && (socketAddress = channel.receive(buffer)) != null) {
                            String sender = "";
                            if (socketAddress instanceof InetSocketAddress) {
                                sender = ((InetSocketAddress) socketAddress).getAddress().toString();
                            }

                            // create a byte array from the buffer
                            buffer.flip();
                            byte bytes[] = new byte[buffer.limit()];
                            buffer.get(bytes, 0, buffer.limit());

                            final Map<String,String> metadata = EventFactoryUtil.createMapWithSender(sender);
                            final E event = eventFactory.create(bytes, metadata, null);
                            events.offer(event);

                            buffer.clear();
                        }
                    }
                }
            } catch (InterruptedException e) {
                stopped = true;
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                logger.error("Error reading from DatagramChannel", e);
            }
        }

        if (buffer != null) {
            try {
                bufferPool.put(buffer);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
