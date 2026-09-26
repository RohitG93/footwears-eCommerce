
using System.Linq.Expressions;

namespace Core.Interfaces;

public interface ISpecificationRepository<T>
{
    Expression<Func<T, bool>>? Criteria { get; }

    Expression<Func<T, object>>? OrderBy { get; }

    Expression<Func<T, object>>? OrderByDescending { get; }

    List<Expression<Func<T, object>>> Includes { get; }
    List<string> IncludeStrings { get; } // For ThenInclude

    bool IsDistinct { get; }

    int PageSize { get; }

    int PageIndex { get; }

    bool IsPagingEnabled { get; }

    IQueryable<T> ApplyCriteria(IQueryable<T> query);    
}

public interface ISpecificationRepository<T, TResult> : ISpecificationRepository<T>
{
    Expression<Func<T, TResult>>? Selector { get; }
}