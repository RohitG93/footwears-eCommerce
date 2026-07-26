

using Core.BusinessEntities;
using Core.Specification;

namespace Core.Specification;

public class ProductSpecification : BaseSpecification<Product>
{
    public ProductSpecification(string? brand, string? type, string? sort)
        : base(p => (string.IsNullOrEmpty(brand) || p.Brand == brand) &&
                    (string.IsNullOrEmpty(type) || p.Type == type))
    {
        if (!string.IsNullOrEmpty(sort))
        {
            switch (sort.ToLower())
            {
                case "price_asc":
                    AddOrderBy(p => p.Price);
                    break;
                case "price_desc":
                    AddOrderByDescending(p => p.Price);
                    break;
                default:
                    AddOrderBy(p => p.Name);
                    break;
            }
        }
        else
        {
            AddOrderBy(p => p.Name);
        }
    }
}