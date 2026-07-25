using Core.BusinessEntities;
namespace Core.Interfaces;

public interface IProductRepository
{
    Task<IEnumerable<Product>> GetAllProductsAsync(string? brand, string? type, string? sort);
    
    Task<List<string>> GetAllProductBrandsAsync();
    Task<List<string>> GetAllProductTypesAsync();
    Task<Product?> GetProductByIdAsync(int id);
    void CreateProduct(Product product);
    Task<bool> UpdateProductAsync(int id, Product product);
    Task<bool> DeleteProductAsync(int id);

    Task<bool> SaveChangesAsync();
}