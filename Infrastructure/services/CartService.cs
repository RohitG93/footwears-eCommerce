
using Core.BusinessEntities;
using Core.Interfaces;
using Microsoft.EntityFrameworkCore.Storage;
using StackExchange.Redis;
using System.Text.Json;

namespace Infrastructure.services;

public class CartService(IConnectionMultiplexer redis) : ICartService
{
    private readonly StackExchange.Redis.IDatabase _database = redis.GetDatabase();
    public async Task<bool> DeleteCartAsync(string key)
    {
        return await _database.KeyDeleteAsync(key);
    }

    public async Task<ShoppingCart?> GetCartAsync(string key)
    {
        var data = await _database.StringGetAsync(key);

        return data.IsNullOrEmpty ? null : JsonSerializer.Deserialize<ShoppingCart>(data!);
    }

    public async Task<ShoppingCart?> SetCartAsync(ShoppingCart cart)
    {
        var created = await _database.StringSetAsync(cart.id,
            JsonSerializer.Serialize(cart), TimeSpan.FromDays(20));

        if (!created) return null;

        return await GetCartAsync(cart.id); 
    }
}