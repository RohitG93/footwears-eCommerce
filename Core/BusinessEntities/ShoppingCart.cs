
using System.ComponentModel.DataAnnotations;

namespace Core.BusinessEntities;
public class ShoppingCart
{
    public required string id {get; set;}

    public List<CartItem> CartItems {get; set;} = [];
}