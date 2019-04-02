package morbrian.nifi.processors.standard;

import morbrian.nifi.processor.util.listen.MulticastDatagramChannelDispatcher;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.listen.AbstractListenEventBatchingProcessor;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.processor.util.listen.event.StandardEventFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static org.apache.nifi.processor.util.listen.ListenerProperties.NETWORK_INTF_NAME;

@SupportsBatching
@Tags({"ingest", "udp", "listen", "multicast", "igmp", "source"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Listens for Datagram Packets on a multicast group. The default behavior produces a FlowFile " +
        "per datagram, however for higher throughput the Max Batch Size property may be increased to specify the number of " +
        "datagrams to batch together in a single FlowFile. This processor can be restricted to listening for datagrams from  a " +
        "specific remote host and port by specifying the Sending Host and Sending Host Port properties, otherwise it will listen " +
        "for datagrams from all hosts and ports.")
@WritesAttributes({
        @WritesAttribute(attribute="udp.sender", description="The sending host of the messages."),
        @WritesAttribute(attribute="udp.port", description="The sending port the messages were received.")
})
public class ListenUdpMulticast extends AbstractListenEventBatchingProcessor<StandardEvent> {

    public static final PropertyDescriptor MULTICAST_GROUP = new PropertyDescriptor.Builder()
            .name("Multicast Group")
            .description("IP of multicast group to join.")
            .addValidator(new HostValidator())
            .expressionLanguageSupported(true)
            .required(true)
            .build();

    public static final String UDP_PORT_ATTR = "udp.port";
    public static final String UDP_SENDER_ATTR = "udp.sender";

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(
                MULTICAST_GROUP
        );
    }

    /**
     * overrides to supply instance of our extended MulticastDatagramChannelDispatcher instead of the base dispatcher.
     */
    @Override
    protected ChannelDispatcher createDispatcher(final ProcessContext context, final BlockingQueue<StandardEvent> events)
            throws IOException {
        final String multicastGroup = context.getProperty(MULTICAST_GROUP).evaluateAttributeExpressions().getValue();
        final String interfaceName = context.getProperty(NETWORK_INTF_NAME).evaluateAttributeExpressions().getValue();
        final Integer bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final BlockingQueue<ByteBuffer> bufferPool = createBufferPool(context.getMaxConcurrentTasks(), bufferSize);
        final EventFactory<StandardEvent> eventFactory = new StandardEventFactory();
        return new MulticastDatagramChannelDispatcher<>(eventFactory, bufferPool, events, getLogger(),
                interfaceName, multicastGroup);
    }

    @Override
    protected Map<String, String> getAttributes(final FlowFileEventBatch batch) {
        final String sender = batch.getEvents().get(0).getSender();
        final Map<String,String> attributes = new HashMap<>(3);
        attributes.put(UDP_SENDER_ATTR, sender);
        attributes.put(UDP_PORT_ATTR, String.valueOf(port));
        return attributes;
    }

    @Override
    protected String getTransitUri(FlowFileEventBatch batch) {
        final String sender = batch.getEvents().get(0).getSender();
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        final String transitUri = new StringBuilder().append("udp").append("://").append(senderHost).append(":")
                .append(port).toString();
        return transitUri;
    }

    public static class HostValidator implements Validator {

        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            try {
                // getByName checks for security exception or host not found
                InetAddress.getByName(input);
                return new ValidationResult.Builder().subject(subject).valid(true).input(input).build();
            } catch (final UnknownHostException e) {
                return new ValidationResult.Builder().subject(subject).valid(false).input(input).explanation("Unknown host: " + e).build();
            }
        }

    }

}
