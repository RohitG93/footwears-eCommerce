

using API.Controllers;
using Core.BusinessEntities;
using Core.Interfaces;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Identity.Client;

public class CartController(ICartService cartService) : BaseApiController
{
    [HttpGet]
    public async Task<ActionResult<ShoppingCart>> GetCart(string id)
    {
        var cartData = await cartService.GetCartAsync(id);

        return Ok(cartData ?? new ShoppingCart{id=id});
    }

    [HttpPost]
    public async Task<ActionResult<ShoppingCart>> UpdateCart(ShoppingCart cartData)
    {
        var cart = await cartService.SetCartAsync(cartData);

        if(cart == null) return BadRequest("Issue during adding data into Cart.");

        return cart;
    }

    [HttpDelete]
    public async Task<ActionResult<bool>> DeleteCart(string Id)
    {
        var cart = await cartService.DeleteCartAsync(Id);

        if(!cart) return BadRequest("Issue during delete data from Cart.");

        return cart;
    }
}