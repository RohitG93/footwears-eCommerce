

using System.Linq.Expressions;
using Core.Interfaces;

namespace Core.Specification;

public class BaseSpecification<T>(Expression<Func<T, bool>>? criteria) : ISpecificationRepository<T>
{

    protected BaseSpecification() : this(null)
    {
        
    }

    public Expression<Func<T, object>>? OrderBy {get; private set;}

    public Expression<Func<T, object>>? OrderByDescending {get; private set;}

    public bool IsDistinct { get; private set; } = false;

    public int PageSize { get; private set; }

    public int PageIndex { get; private set; }

    public bool IsPagingEnabled { get; private set; } = false;


    Expression<Func<T, bool>>? ISpecificationRepository<T>.Criteria => criteria;

    protected void AddOrderBy(Expression<Func<T, object>> orderByExpression)
    {
        OrderBy = orderByExpression;
    }

    protected void AddOrderByDescending(Expression<Func<T, object>> orderByDescendingExpression)
    {
        OrderByDescending = orderByDescendingExpression;
    }

    protected void ApplyDistinct()
    {
        IsDistinct = true;
    }

    protected void ApplyPaging(int pageIndex, int pageSize)
    {
        PageIndex = pageIndex;
        PageSize = pageSize;
        IsPagingEnabled = true;
    }

    IQueryable<T> ISpecificationRepository<T>.ApplyCriteria(IQueryable<T> query)
    {
        if (criteria != null)
        {
            return query.Where(criteria);
        }

        return query;
    }
}

public class BaseSpecification<T, TResult>(Expression<Func<T, bool>>? criteria) : 
    BaseSpecification<T>(criteria), ISpecificationRepository<T, TResult>
{
    protected BaseSpecification() : this(null)
    {
        
    }

    public Expression<Func<T, TResult>>? Selector {get; private set;}

    protected void AddSelector(Expression<Func<T, TResult>> selectorExpression)
    {
        Selector = selectorExpression;
    }
}