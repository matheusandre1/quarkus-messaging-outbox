package io.quarkiverse.messaging.outbox.sample;

import java.util.List;

import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.quarkiverse.messaging.outbox.OutboxEmitter;

@Path("/customers")
public class CustomerResource {

    @Inject
    @Channel("customer-events")
    OutboxEmitter<CustomerCreated> events;

    @Inject
    CustomerEventLogger eventLogger;

    @POST
    @Transactional
    public Customer create(@QueryParam("name") String name, @QueryParam("email") String email) {
        Customer customer = new Customer();
        customer.name = name;
        customer.email = email;
        customer.persist();

        events.sendAndAwait(customer.id.toString(), new CustomerCreated(customer.id, customer.name));
        return customer;
    }

    @GET
    public List<Customer> list() {
        return Customer.listAll();
    }

    @GET
    @Path("/events/count")
    public int eventsCount() {
        return eventLogger.getReceived().size();
    }
}
