package ru.yandex.practicum.telemetry.collector.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Глобальный обработчик ошибок: превращает исключения в понятные HTTP-ответы.
 */
@Slf4j
@RestControllerAdvice
public class ErrorHandler {

    /** Тело запроса не прошло валидацию (@NotBlank, @NotNull, @NotEmpty и т.д.). */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String handleValidation(MethodArgumentNotValidException e) {
        log.warn("Ошибка валидации: {}", e.getMessage());
        return "Ошибка валидации: " + e.getMessage();
    }

    /** Некорректный JSON: неизвестный type, неверный формат даты, не тот тип поля и т.п. */
    @ExceptionHandler({HttpMessageNotReadableException.class, IllegalArgumentException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String handleBadRequest(Exception e) {
        log.warn("Некорректный запрос: {}", e.getMessage());
        return "Некорректное тело запроса: " + e.getMessage();
    }

}
