package by.egor.warehouseservice.entity;

public record Order(
        String orderId,
        String product,
        Integer quantity
) {
}
