package org.example.registrationservice.models.services.impl;

import feign.FeignException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.registrationservice.clients.EventClient;
import org.example.registrationservice.exceptions.EventNotFoundException;
import org.example.registrationservice.exceptions.EventServiceException;
import org.example.registrationservice.models.dto.responses.EventResponse;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class EventGatewayService {

    private final EventClient eventClient;

    @CircuitBreaker(name = "eventService", fallbackMethod = "getEventByIdFallback")
    public EventResponse getEventById(Long eventId) {
        EventResponse event = eventClient.getEventById(eventId);

        if (event == null || event.id() == null || event.ticketPrice() == null || event.availableTickets() == null) {
            throw new EventServiceException("Invalid event data returned by Event Service for ID: " + eventId);
        }

        return event;
    }

    private EventResponse getEventByIdFallback(Long eventId, Throwable throwable) {
        if (throwable instanceof EventNotFoundException eventNotFoundException) {
            throw eventNotFoundException;
        }

        if (throwable instanceof EventServiceException eventServiceException) {
            throw eventServiceException;
        }

        if (throwable instanceof FeignException feignException
                && feignException.status() == HttpStatus.NOT_FOUND.value()) {
            throw new EventNotFoundException(eventId);
        }

        log.error("Event Service call failed for event ID: {}", eventId, throwable);
        throw new EventServiceException("Event Service is unavailable, cannot retrieve event with ID: " + eventId, throwable);
    }
}
