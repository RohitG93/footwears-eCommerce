using Infrastructure.Data;
using Microsoft.AspNetCore.Mvc;
using Core.BusinessEntities;
using Core.Interfaces;
using Core.Specification;
using API.RequestHelpers;

namespace API.Controllers;

public class ProductsController(IGenericRepository<Product> productRepository) : BaseApiController
{

    [HttpGet]
    public async Task<ActionResult<Pagination<Product>>> GetAllProducts([FromQuery]ProductSpecParam productSpecParam)
    {
        var productSpec = new ProductSpecification(productSpecParam);
        return await CreatePagedResult(productRepository, productSpec,productSpecParam.PageIndex, productSpecParam.PageSize);
    }

    [HttpGet("brands")]
    public async Task<ActionResult<IEnumerable<string>>> GetAllProductBrands()
    {
        //return Ok(await productRepository.GetAllProductBrandsAsync());

        var productBrandSpec = new ProductBrandSpecification();

        return Ok(await productRepository.ListAsync(productBrandSpec));
    }

    [HttpGet("types")]
    public async Task<ActionResult<IEnumerable<string>>> GetAllProductTypes()
    {
        //return Ok(await productRepository.GetAllProductTypesAsync());

        var productTypeSpec = new ProductTypeSpecification();

        return Ok(await productRepository.ListAsync(productTypeSpec));

    }

    [HttpGet("{id:int}")]
    public async Task<ActionResult<Product>> GetProductById(int id)
    {
        var product = await productRepository.GetByIdAsync(id);
        if (product == null)
        {
            return NotFound();
        }
        return product;
    }

    [HttpPost]
    public async Task<ActionResult<IEnumerable<Product>>> CreateProducts(Product product)
    {
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
        if (await productRepository.DeleteAsync(id))
        {
            if (await productRepository.SaveChangesAsync())
            {
                return NoContent();
            }
            return BadRequest("During deletion, something went wrong. Please try again.");
        }

        return NotFound();
    }
}