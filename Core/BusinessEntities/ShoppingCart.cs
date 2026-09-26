
using System.ComponentModel.DataAnnotations;

namespace Core.BusinessEntities;
public class ShoppingCart
{
    public required string id {get; set;}

    public List<CartItem> cartItems {get; set;} = [];

    public int? DeliveryMethodId { get; set; }
    public string? ClientSecret { get; set; }
    public string? PaymentIntentId { get; set; }


}