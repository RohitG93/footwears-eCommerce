using Infrastructure.Data;
using Microsoft.AspNetCore.Mvc;
using Core.BusinessEntities;
using Core.Interfaces;
using Core.Specification;
using API.RequestHelpers;

namespace API.Controllers;

public class ProductsController(IUnitOfWork unit) : BaseApiController
{

    [HttpGet]
    public async Task<ActionResult<Pagination<Product>>> GetAllProducts([FromQuery]ProductSpecParam productSpecParam)
    {
        var productSpec = new ProductSpecification(productSpecParam);
        var productRepository = unit.Repository<Product>();
        return await CreatePagedResult(productRepository, productSpec,productSpecParam.PageIndex, productSpecParam.PageSize);
    }

    [HttpGet("brands")]
    public async Task<ActionResult<IEnumerable<string>>> GetAllProductBrands()
    {
        //return Ok(await productRepository.GetAllProductBrandsAsync());

        var productBrandSpec = new ProductBrandSpecification();

        return Ok(await unit.Repository<Product>().ListAsync(productBrandSpec));
    }

    [HttpGet("types")]
    public async Task<ActionResult<IEnumerable<string>>> GetAllProductTypes()
    {
        //return Ok(await productRepository.GetAllProductTypesAsync());

        var productTypeSpec = new ProductTypeSpecification();

        return Ok(await unit.Repository<Product>().ListAsync(productTypeSpec));

    }

    [HttpGet("{id:int}")]
    public async Task<ActionResult<Product>> GetProductById(int id)
    {
        var product = await unit.Repository<Product>().GetByIdAsync(id);
        if (product == null)
        {
            return NotFound();
        }
        return product;
    }

    [HttpPost]
    public async Task<ActionResult<IEnumerable<Product>>> CreateProducts(Product product)
    {
        var productRepository = unit.Repository<Product>();
        productRepository.Create(product);
        if(await productRepository.SaveChangesAsync())
        {
            return CreatedAtAction(nameof(GetProductById), new { id = product.Id }, product);
        }
        return BadRequest("During creation, something went wrong. Please try again.");    
    }

    [HttpPut("{id:int}")]
    public async Task<ActionResult<IEnumerable<Product>>> UpdateProduct(int id, Product product)
    {
        if (id != product.Id)
        {
            return BadRequest("Product ID mismatch.");
        }

        var productRepository = unit.Repository<Product>();
        if (await productRepository.UpdateAsync(id, product))
        {
            if (await productRepository.SaveChangesAsync())
            {
                return NoContent();
            }
            return BadRequest("During update, something went wrong. Please try again.");
        }

        return NotFound("Product not found.");   
    }

    [HttpDelete("{id:int}")]
    public async Task<ActionResult<IEnumerable<Product>>> DeleteProduct(int id)
    {        
        if (await unit.Repository<Product>().DeleteAsync(id))
        {
            if (await unit.Repository<Product>().SaveChangesAsync())
            {
                return NoContent();
            }
            return BadRequest("During deletion, something went wrong. Please try again.");
        }

        return NotFound();
    }
}