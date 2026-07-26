

using Core.BusinessEntities;
using Core.Specification;

namespace Core.Specification;

public class ProductSpecification : BaseSpecification<Product>
{
    public ProductSpecification(ProductSpecParam specParams)
        : base(p => (string.IsNullOrEmpty(specParams.Search) || p.Name.ToLower().Contains(specParams.Search)) &&
                    (specParams.Brands.Count == 0 || specParams.Brands.Contains(p.Brand)) &&
                    (specParams.Types.Count == 0 || specParams.Types.Contains(p.Type)))
    {
        if (!string.IsNullOrEmpty(specParams.Sort))
        {
            switch (specParams.Sort.ToLower())
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

        ApplyPaging(specParams.PageIndex, specParams.PageSize);
    }
}