using System.Collections.Generic;
using System.Threading.Tasks;
using Core.BusinessEntities;
using Core.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace Infrastructure.Data;

public class ProductRepository : IProductRepository
{
    private readonly StoreContext _storeContext;

    public ProductRepository(StoreContext storeContext)
    {
        _storeContext = storeContext;
    }

    public async Task<IEnumerable<Product>> GetAllProductsAsync(string? brand, string? type, string? sort)
    {
        var query = _storeContext.Products.AsQueryable();

        if (!string.IsNullOrEmpty(brand))
        {
            query = query.Where(p => p.Brand == brand);
        }

        if (!string.IsNullOrEmpty(type))
        {
            query = query.Where(p => p.Type == type);
        }

        switch (sort)
        {
            case "price_asc":
                query = query.OrderBy(p => p.Price);
                break;
            case "price_desc":
                query = query.OrderByDescending(p => p.Price);
                break;
            default:
                query = query.OrderBy(p => p.Name);
                break;
        }

        return await query.ToListAsync();
    }

    public async Task<Product?> GetProductByIdAsync(int id)
    {
        return await _storeContext.Products.FindAsync(id);
    }

    public void CreateProduct(Product product)
    {
        _storeContext.Products.Add(product);
    }

    public async Task<bool> UpdateProductAsync(int id, Product product)
    {
        var existingProduct = await _storeContext.Products.AsNoTracking().AnyAsync(p => p.Id == id);
        if (!existingProduct)
        {
            return false;
        }
        _storeContext.Entry(product).State = EntityState.Modified;
        return true;
    }

    public async Task<bool> DeleteProductAsync(int id)
    {
        var product = await _storeContext.Products.FindAsync(id);
        if (product == null)
        {
            return false;
        }
        _storeContext.Products.Remove(product);
        return true;
    }

    public async Task<bool> SaveChangesAsync()
    {
        return (await _storeContext.SaveChangesAsync()) > 0;
    }

    public async Task<List<string>> GetAllProductBrandsAsync()
    {
        return await _storeContext.Products.Select(p => p.Brand)
            .Distinct()
            .ToListAsync();
    }

    public async Task<List<string>> GetAllProductTypesAsync()
    {
        return await _storeContext.Products.Select(p => p.Type).Distinct().ToListAsync();
    }
}