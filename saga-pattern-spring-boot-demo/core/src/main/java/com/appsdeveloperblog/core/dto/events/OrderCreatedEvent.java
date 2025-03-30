package com.appsdeveloperblog.core.dto.events;

import java.util.UUID;

public class OrderCreatedEvent {
    private UUID orderId;
    private UUID customerId;
    private UUID productId;
    private int productQuantity;

    public OrderCreatedEvent() {
    }

    public OrderCreatedEvent(UUID orderId, UUID customerId, UUID productId, int productQuantity) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.productQuantity = productQuantity;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public void setOrderId(UUID orderId) {
        this.orderId = orderId;
    }

    public UUID getCustomerId() {
        return customerId;
    }

    public void setCustomerId(UUID customerId) {
        this.customerId = customerId;
    }

    public UUID getProductId() {
        return productId;
    }

    public void setProductId(UUID productId) {
        this.productId = productId;
    }

    public int getProductQuantity() {
        return productQuantity;
    }

    public void setProductQuantity(int productQuantity) {
        this.productQuantity = productQuantity;
    }
}
