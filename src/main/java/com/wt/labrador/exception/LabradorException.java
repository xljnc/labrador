package com.wt.labrador.exception;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 一贫
 * @date 2021/9/9
 */
@Data
public class LabradorException extends RuntimeException {

    private String code;

    private String message;

    public LabradorException() {
    }

    public LabradorException(String message) {
        super(message);
        this.message = message;
    }

    public LabradorException(String code, String message) {
        super(message);
        this.code = code;
        this.message = message;
    }
}
