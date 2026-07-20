package it.unibo.sap.delivery.application;

public final class DeliveryExceptions {

    private DeliveryExceptions() {
    }

    public static class ValidationRejectedException extends RuntimeException {
        public ValidationRejectedException(final String reason) {
            super(reason);
        }
    }

    public static class BadRequestException extends RuntimeException {
        public BadRequestException(final String message) {
            super(message);
        }
    }

    public static class DeliveryNotFoundException extends RuntimeException {
        public DeliveryNotFoundException() {
            super("Delivery not found");
        }
    }

    public static class CannotCancelInFlightException extends RuntimeException {
        public CannotCancelInFlightException() {
            super("Delivery cannot be cancelled once in flight");
        }
    }

    /**
     * A Sender acting on a delivery that is not their own. Ported from A2: on the CANCEL command an
     * explicit 403 is more diagnostically honest than a 404-hide, because the caller already knows the
     * delivery id it is acting on. Reads (get/track) deliberately keep the 404-hide instead, so a 403
     * would not confirm the delivery's existence to a stranger.
     */
    public static class ForbiddenDeliveryAccessException extends RuntimeException {
        public ForbiddenDeliveryAccessException() {
            super("You can only cancel your own delivery");
        }
    }

    /** Missing or invalid session identity on a command (STEP 5 authorization). Maps to 401. */
    public static class UnauthorizedException extends RuntimeException {
        public UnauthorizedException(final String message) {
            super(message);
        }
    }

    /** Valid session but wrong role for the requested command (STEP 5 authorization). Maps to 403. */
    public static class ForbiddenException extends RuntimeException {
        public ForbiddenException(final String message) {
            super(message);
        }
    }
}
