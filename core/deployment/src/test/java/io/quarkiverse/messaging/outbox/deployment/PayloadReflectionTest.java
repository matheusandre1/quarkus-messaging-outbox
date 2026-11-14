package io.quarkiverse.messaging.outbox.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;
import org.junit.jupiter.api.Test;

/** Which classes the build registers for reflection, given an OutboxEmitter payload type. */
class PayloadReflectionTest {

    interface Event {
    }

    static class Order implements Event {
    }

    static class PriorityOrder extends Order {
    }

    static class Refund implements Event {
    }

    private static Type type(Class<?> c) {
        return Type.create(DotName.createSimple(c.getName()), Type.Kind.CLASS);
    }

    private static Set<String> collect(Type type) throws Exception {
        Index index = Index.of(Event.class, Order.class, PriorityOrder.class, Refund.class);
        Set<String> classes = new TreeSet<>();
        MessagingOutboxProcessor.collectPayloadClasses(type, index, classes);
        return classes;
    }

    @Test
    void classIsRegisteredWithItsSubclasses() throws Exception {
        assertThat(collect(type(Order.class)))
                .containsExactlyInAnyOrder(Order.class.getName(), PriorityOrder.class.getName());
    }

    @Test
    void interfaceIsRegisteredWithItsImplementations() throws Exception {
        assertThat(collect(type(Event.class))).containsExactlyInAnyOrder(Event.class.getName(), Order.class.getName(),
                PriorityOrder.class.getName(), Refund.class.getName());
    }

    @Test
    void typeArgumentsAreRegisteredAndJdkTypesAreSkipped() throws Exception {
        Type listOfOrders = ParameterizedType.create(DotName.createSimple(List.class.getName()),
                new Type[] { type(Order.class) }, null);
        assertThat(collect(listOfOrders))
                .containsExactlyInAnyOrder(Order.class.getName(), PriorityOrder.class.getName());
        assertThat(collect(type(String.class))).isEmpty();
    }
}
