package com.sb.kafka.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
public class MessageEntity {

    private String type;

    @JsonFormat(pattern = "dd/MM/yyyy")
    private LocalDate time;
}
