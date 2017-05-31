package io.hekate.messaging.internal;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterJoinRejectedException;
import io.hekate.core.HekateFutureException;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingServiceFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MessagingClusterJoinRejectTest extends HekateNodeTestBase {
    private static final int PARTITIONS = 128;

    private static final int BACKUP_NODES = 10;

    private HekateTestNode existing;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        // Emulate existing cluster.
        existing = createNode(boot ->
            boot.withService(MessagingServiceFactory.class, messaging ->
                messaging.withChannel(MessagingChannelConfig.of(Object.class)
                    .withName("test")
                    .withPartitions(PARTITIONS)
                    .withBackupNodes(BACKUP_NODES)
                    .withReceiver(msg ->
                        fail("Messages are not expected.")
                    )
                )
            )
        ).join();
    }

    @Test
    public void testChannelTypeMismatchNoReceiver() throws Exception {
        doTestChannelTypeMismatch(null);
    }

    @Test
    public void testChannelTypeMismatchWithReceiver() throws Exception {
        doTestChannelTypeMismatch(msg ->
            fail("Messages are not expected.")
        );
    }

    @Test
    public void testPartitionsMismatchNoReceiver() throws Exception {
        doTestPartitionsMismatch(null);
    }

    @Test
    public void testPartitionsMismatchWithReceiver() throws Exception {
        doTestPartitionsMismatch(msg ->
            fail("Messages are not expected.")
        );
    }

    @Test
    public void testBackupNodesMismatchNoReceiver() throws Exception {
        doTestBackupNodesMismatch(null);
    }

    @Test
    public void testBackupNodesMismatchWithReceiver() throws Exception {
        doTestBackupNodesMismatch(msg ->
            fail("Messages are not expected.")
        );
    }

    private void doTestPartitionsMismatch(MessageReceiver<Object> receiver) throws Exception {
        try {
            createNode(boot ->
                boot.withService(MessagingServiceFactory.class, messaging ->
                    messaging.withChannel(MessagingChannelConfig.unchecked()
                        .withName("test")
                        .withPartitions(PARTITIONS * 2)
                        .withBackupNodes(BACKUP_NODES)
                        .withReceiver(receiver)
                    )
                )
            ).join();

            fail("Error not thrown.");
        } catch (HekateFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(ClusterJoinRejectedException.class));
            assertEquals(
                "Invalid " + MessagingChannelConfig.class.getSimpleName() + " - "
                    + "'partitions' value mismatch between the joining node and the cluster "
                    + "[channel=test"
                    + ", joining-value=" + PARTITIONS * 2
                    + ", cluster-value=" + PARTITIONS
                    + ", rejected-by=" + existing.localNode().address()
                    + "]",
                e.findCause(ClusterJoinRejectedException.class).rejectReason()
            );
        }
    }

    private void doTestBackupNodesMismatch(MessageReceiver<Object> receiver) throws Exception {
        try {
            createNode(boot ->
                boot.withService(MessagingServiceFactory.class, messaging ->
                    messaging.withChannel(MessagingChannelConfig.unchecked()
                        .withName("test")
                        .withPartitions(PARTITIONS)
                        .withBackupNodes(BACKUP_NODES * 2)
                        .withReceiver(receiver)
                    )
                )
            ).join();

            fail("Error not thrown.");
        } catch (HekateFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(ClusterJoinRejectedException.class));
            assertEquals(
                "Invalid " + MessagingChannelConfig.class.getSimpleName() + " - "
                    + "'backupNodes' value mismatch between the joining node and the cluster "
                    + "[channel=test"
                    + ", joining-value=" + BACKUP_NODES * 2
                    + ", cluster-value=" + BACKUP_NODES
                    + ", rejected-by=" + existing.localNode().address()
                    + "]",
                e.findCause(ClusterJoinRejectedException.class).rejectReason()
            );
        }
    }

    private void doTestChannelTypeMismatch(MessageReceiver<?> receiver) throws Exception {
        @SuppressWarnings("unchecked")
        MessageReceiver<String> mayBeReceiver = (MessageReceiver<String>)receiver;

        try {
            createNode(boot ->
                boot.withService(MessagingServiceFactory.class, messaging -> {
                        messaging.withChannel(MessagingChannelConfig.of(String.class)
                            .withName("test")
                            .withPartitions(PARTITIONS)
                            .withBackupNodes(BACKUP_NODES)
                            .withReceiver(mayBeReceiver)
                        );
                    }
                )
            ).join();

            fail("Error not thrown.");
        } catch (HekateFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(ClusterJoinRejectedException.class));
            assertEquals(
                "Invalid " + MessagingChannelConfig.class.getSimpleName() + " - "
                    + "'baseType' value mismatch between the joining node and the cluster "
                    + "[channel=test"
                    + ", joining-type=java.lang.String"
                    + ", cluster-type=java.lang.Object"
                    + ", rejected-by=" + existing.localNode().address()
                    + "]",
                e.findCause(ClusterJoinRejectedException.class).rejectReason()
            );
        }
    }
}
