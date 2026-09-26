using System.Runtime.Serialization;

namespace Core.BusinessEntities.OrderAggregate;

public enum OrderStatus
{
    Pending,
    PaymentReceived,
    PaymentFailed,
    PaymentMismatch,
    Refunded
}