package org.example.registrationservice.exceptions;

public class InsufficientTicketsException extends RuntimeException {
    public InsufficientTicketsException(Long eventId, Integer requestedQuantity, Integer availableTickets) {
        super("Not enough tickets for event with ID: " + eventId
                + " (requested: " + requestedQuantity + ", available: " + availableTickets + ")");
    }
}
