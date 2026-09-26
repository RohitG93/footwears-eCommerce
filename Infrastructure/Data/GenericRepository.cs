using Core.BusinessEntities;
using Core.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace Infrastructure.Data;

public class GenericRepository<T> : IGenericRepository<T> where T : BaseEntity
{
    private readonly StoreContext _context;
    private readonly DbSet<T> _dbSet;

    public GenericRepository(StoreContext context)
    {
        _context = context;
        _dbSet = _context.Set<T>();
    }

    public async Task<IEnumerable<T>> GetAllAsync()
    {
        return await _dbSet.ToListAsync();
    }

    public async Task<T?> GetByIdAsync(int id)
    {
        return await _dbSet.FindAsync(id);
    }

    public void Create(T entity)
    {
        _dbSet.Add(entity);

    }

    public async Task<bool> UpdateAsync(int id, T entity)
    {
        var existingEntity = await IsExistsAsync(id);
        if (!existingEntity)
        {
            return false;
        }

        _dbSet.Attach(entity);
        _context.Entry(entity).State = EntityState.Modified;
        return true;
    }

    public async Task<bool> DeleteAsync(int id)
    {
        var entity = await GetByIdAsync(id);
        if (entity == null)
        {
            return false;
        }
        _dbSet.Remove(entity);
        return true;
    }

    public async Task<bool> SaveChangesAsync()
    {
        return await _context.SaveChangesAsync() > 0;
    }

    public async Task<bool> IsExistsAsync(int id)
    {
        return await _dbSet.AnyAsync(e => e.Id == id);
    }

    public async Task<IEnumerable<T>> GetAllDataWithSpecAsync(ISpecificationRepository<T> spec)
    {
        return await ApplySpecification(spec).ToListAsync();
    }

    public async Task<T?> GetDataWithSpecAsync(ISpecificationRepository<T> spec)
    {
        return await ApplySpecification(spec).FirstOrDefaultAsync();
    }

    public async Task<TResult?> GetEntityWithSpec<TResult>(ISpecificationRepository<T, TResult> spec)
    {
        return await ApplySpecification(spec).FirstOrDefaultAsync();
    }

    public async Task<IEnumerable<TResult>> ListAsync<TResult>(ISpecificationRepository<T, TResult> spec)
    {
        return await ApplySpecification(spec).ToListAsync();
    }

    public async Task<int> CountAsAsync(ISpecificationRepository<T> spec)
    {
        var query = _dbSet.AsQueryable();

        query = spec.ApplyCriteria(query);

        return await query.CountAsync();
    }

    public async Task<T?> GetEntityWithSpec(ISpecificationRepository<T> spec)
    {
        return await ApplySpecification(spec).FirstOrDefaultAsync();
    }

    public async Task<IReadOnlyList<T>> ListAsync(ISpecificationRepository<T> spec)
    {
        return await ApplySpecification(spec).ToListAsync();
    }

    private IQueryable<T> ApplySpecification(ISpecificationRepository<T> spec)
    {
        return SpecificationEvaluator<T>.GetQuery(_dbSet.AsQueryable(), spec);
    }

    private IQueryable<TResult> ApplySpecification<TResult>(ISpecificationRepository<T, TResult> spec)
    {
        return SpecificationEvaluator<T>.GetQuery(_dbSet.AsQueryable(), spec);
    }
}