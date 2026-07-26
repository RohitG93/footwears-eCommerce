

using API.RequestHelpers;
using Core.BusinessEntities;
using Core.Interfaces;
using Core.Specification;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers;

[ApiController]
[Route("api/[controller]")] 
public class BaseApiController : ControllerBase
{
    protected async Task<ActionResult> CreatePagedResult<T>(IGenericRepository<T> repository, 
        ISpecificationRepository<T> spec, int pageIndex, int pageSize) where T : BaseEntity
    {
        var productData = await repository.GetAllDataWithSpecAsync(spec);
        var count = await repository.CountAsAsync(spec);

        var paginationData = new Pagination<T>(pageIndex, pageSize, count, productData);

        return Ok(paginationData);
    }
}