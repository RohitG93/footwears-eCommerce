using Core.BusinessEntities;

namespace Core.Interfaces;

public interface IGenericRepository<T> where T : BaseEntity
{
    Task<IEnumerable<T>> GetAllAsync();
    Task<T?> GetByIdAsync(int id);
    void Create(T entity);
    Task<bool> UpdateAsync(int id, T entity);
    Task<bool> DeleteAsync(int id);
    Task<bool> SaveChangesAsync();

    Task<bool> IsExistsAsync(int id);

    Task<IEnumerable<T>> GetAllDataWithSpecAsync(ISpecificationRepository<T> spec);

    Task<T?> GetDataWithSpecAsync(ISpecificationRepository<T> spec);

    Task<TResult?> GetEntityWithSpec<TResult>(ISpecificationRepository<T, TResult> spec);
    Task<IEnumerable<TResult>> ListAsync<TResult>(ISpecificationRepository<T, TResult> spec);

    Task<int> CountAsAsync(ISpecificationRepository<T> spec);

    Task<T?> GetEntityWithSpec(ISpecificationRepository<T> spec);
    Task<IReadOnlyList<T>> ListAsync(ISpecificationRepository<T> spec);



}