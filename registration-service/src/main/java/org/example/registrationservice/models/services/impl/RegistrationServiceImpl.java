package org.example.registrationservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.registrationservice.exceptions.InsufficientTicketsException;
import org.example.registrationservice.models.constants.RegistrationStatus;
import org.example.registrationservice.models.dto.requests.CreateRegistrationDetailRequest;
import org.example.registrationservice.models.dto.requests.CreateRegistrationRequest;
import org.example.registrationservice.models.dto.responses.EventResponse;
import org.example.registrationservice.models.dto.responses.RegistrationDetailResponse;
import org.example.registrationservice.models.dto.responses.RegistrationResponse;
import org.example.registrationservice.models.entities.Registration;
import org.example.registrationservice.models.entities.RegistrationDetail;
import org.example.registrationservice.models.repositories.RegistrationDetailRepository;
import org.example.registrationservice.models.repositories.RegistrationRepository;
import org.example.registrationservice.models.services.RegistrationService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class RegistrationServiceImpl implements RegistrationService {

    private static final String REGISTRATION_CREATED_TOPIC = "registration-created";

    private final RegistrationRepository registrationRepository;
    private final RegistrationDetailRepository registrationDetailRepository;
    private final EventGatewayService eventGatewayService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Override
    @Transactional
    public RegistrationResponse createRegistration(CreateRegistrationRequest request) {
        List<RegistrationDetail> registrationDetails = new ArrayList<>();
        double totalAmount = 0d;

        for (CreateRegistrationDetailRequest item : request.items()) {
            Integer quantity = item.quantity();
            if (quantity == null || quantity <= 0) {
                throw new IllegalArgumentException("Quantity must be greater than 0");
            }

            EventResponse event = eventGatewayService.getEventById(item.eventId());

            if (quantity > event.availableTickets()) {
                throw new InsufficientTicketsException(event.id(), quantity, event.availableTickets());
            }

            double lineTotal = event.ticketPrice() * quantity;
            totalAmount += lineTotal;

            registrationDetails.add(RegistrationDetail.builder()
                    .eventId(event.id())
                    .quantity(quantity)
                    .ticketPrice(event.ticketPrice())
                    .lineTotal(lineTotal)
                    .build());
        }

        Registration registration = registrationRepository.save(Registration.builder()
                .participantName(request.participantName())
                .participantEmail(request.participantEmail())
                .totalAmount(totalAmount)
                .status(RegistrationStatus.PENDING)
                .build());

        registrationDetails.forEach(detail -> detail.setRegistration(registration));
        List<RegistrationDetail> savedDetails = registrationDetailRepository.saveAll(registrationDetails);

        kafkaTemplate.send(REGISTRATION_CREATED_TOPIC, registration.getParticipantEmail());

        return new RegistrationResponse(
                registration.getId(),
                registration.getParticipantName(),
                registration.getParticipantEmail(),
                registration.getTotalAmount(),
                registration.getStatus(),
                savedDetails.stream().map(this::toDetailResponse).toList());
    }

    private RegistrationDetailResponse toDetailResponse(RegistrationDetail detail) {
        return new RegistrationDetailResponse(
                detail.getId(),
                detail.getEventId(),
                detail.getQuantity(),
                detail.getTicketPrice(),
                detail.getLineTotal());
    }
}
