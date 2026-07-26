using Core.BusinessEntities;

namespace Core.Specification;

public class ProductTypeSpecification : BaseSpecification<Product, string>
{
    public ProductTypeSpecification()
    {
        AddSelector(p => p.Type);
        ApplyDistinct();
    }
}