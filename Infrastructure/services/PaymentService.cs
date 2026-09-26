using Core.BusinessEntities;
using Core.Interfaces;
using Microsoft.Extensions.Configuration;
using Stripe;

namespace Infrastructure.Services;

public class PaymentService : IPaymentService
{
    private readonly ICartService cartService;

    private readonly IGenericRepository<Core.BusinessEntities.Product> products;

    private readonly IGenericRepository<Core.BusinessEntities.DeliveryMethod> deliveryMethods;

    public PaymentService(IConfiguration config, ICartService cartService, 
        IGenericRepository<Core.BusinessEntities.Product> products,
        IGenericRepository<Core.BusinessEntities.DeliveryMethod> deliveryMethods)
    {
        StripeConfiguration.ApiKey = config["StripeSettings:SecretKey"];
        this.cartService = cartService;
        this.products = products;
        this.deliveryMethods = deliveryMethods;
    }

    public async Task<ShoppingCart?> CreateOrUpdatePaymentIntent(string cartId)
    {
        var cart = await cartService.GetCartAsync(cartId)
            ?? throw new Exception("Cart unavailable");

        var shippingPrice = await GetShippingPriceAsync(cart) ?? 0;

        await ValidateCartItemsInCartAsync(cart);

        var subtotal = CalculateSubtotal(cart);

        var total = subtotal + shippingPrice;

        await CreateUpdatePaymentIntentAsync(cart, total);

        await cartService.SetCartAsync(cart);

        return cart;
    }
    
    public async Task<string> RefundPayment(string paymentIntentId)
    {
        var refundOptions = new RefundCreateOptions
        {
            PaymentIntent = paymentIntentId
        };

        var refundService = new RefundService();
        var result = await refundService.CreateAsync(refundOptions);

        return result.Status;
    }

    private async Task CreateUpdatePaymentIntentAsync(ShoppingCart cart,
        long total)
    {
        var service = new PaymentIntentService();

        if (string.IsNullOrEmpty(cart.PaymentIntentId))
        {
            var options = new PaymentIntentCreateOptions
            {
                Amount = total,
                Currency = "usd",
                PaymentMethodTypes = ["card"]
            };
            var intent = await service.CreateAsync(options);
            cart.PaymentIntentId = intent.Id;
            cart.ClientSecret = intent.ClientSecret;
        }
        else
        {
            var options = new PaymentIntentUpdateOptions
            {
                Amount = total
            };
            await service.UpdateAsync(cart.PaymentIntentId, options);
        }
    }

    // private async Task<long> ApplyDiscountAsync(AppCoupon appCoupon, 
	//     long amount)
    // {
    //     var couponService = new Stripe.CouponService();

    //     var coupon = await couponService.GetAsync(appCoupon.CouponId);

    //     if (coupon.AmountOff.HasValue)
    //     {
    //         amount -= (long)coupon.AmountOff * 100;
    //     }

    //     if (coupon.PercentOff.HasValue)
    //     {
    //         var discount = amount * (coupon.PercentOff.Value / 100);
    //         amount -= (long)discount;
    //     }

    //     return amount;
    // }

    private long CalculateSubtotal(ShoppingCart cart)
    {
        var itemTotal = cart.cartItems.Sum(x => x.Quantity * x.Price * 100);
        return (long)itemTotal;
    }

    private async Task ValidateCartItemsInCartAsync(ShoppingCart cart)
    {
        foreach (var item in cart.cartItems)
        {
            var productItem = await products
                .GetByIdAsync(item.ProductId) 
	                ?? throw new Exception("Problem getting product in cart");

            if (item.Price != productItem.Price)
            {
                item.Price = productItem.Price;
            }
        }
    }

    private async Task<long?> GetShippingPriceAsync(ShoppingCart cart)
    {
        if (cart.DeliveryMethodId.HasValue)
        {
            var deliveryMethod = await deliveryMethods
                .GetByIdAsync((int)cart.DeliveryMethodId)
                    ?? throw new Exception("Problem with delivery method");

            return (long)deliveryMethod.Price * 100;
        }

        return null;
    }
}