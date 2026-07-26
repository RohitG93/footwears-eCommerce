
using Core.Specifications;

namespace Core.Specification;
public class ProductSpecParam : PaginationParams
{
    private List<string> _brands = new List<string>();
    private List<string> _types = new List<string>();
    public List<string> Brands
    {
        get => _brands;
        set
        {
            _brands = value.SelectMany(b => b.Split(',',
                StringSplitOptions.RemoveEmptyEntries)).ToList();
        }
    }
    public List<string> Types
    {
        get => _types;
        set
        {
            _types = value.SelectMany(t => t.Split(',',
                StringSplitOptions.RemoveEmptyEntries)).ToList();
        }
    }

    public string? Sort { get; set; }

    private string? _search { get; set; }

    public string Search
    {
        get => _search ?? "";
        set
        {
            _search = value.ToLower();
        }
    }

}