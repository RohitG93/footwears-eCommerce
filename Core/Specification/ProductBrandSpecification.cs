using Core.BusinessEntities;

namespace Core.Specification;

public class ProductBrandSpecification : BaseSpecification<Product, string>
{
    public ProductBrandSpecification()
    {
        AddSelector(p => p.Brand);
        ApplyDistinct();
    }
}