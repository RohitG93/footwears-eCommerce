using Infrastructure.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Core.BusinessEntities;
using Core.Interfaces;

namespace API.Controllers;

[ApiController]
[Route("api/[controller]")] 
public class ProductsController(IProductRepository productRepository) : ControllerBase
{

    [HttpGet]
    public async Task<ActionResult<IEnumerable<Product>>> GetAllProducts(string? brand, string? type, string? sort)
    {
        return Ok(await productRepository.GetAllProductsAsync(brand, type, sort));
    }

    [HttpGet("brands")]
    public async Task<ActionResult<IEnumerable<string>>> GetAllProductBrands()
    {
        return Ok(await productRepository.GetAllProductBrandsAsync());
    }

    [HttpGet("types")]
    public async Task<ActionResult<IEnumerable<string>>> GetAllProductTypes()
    {
        return Ok(await productRepository.GetAllProductTypesAsync());
    }

    [HttpGet("{id:int}")]
    public async Task<ActionResult<IEnumerable<Product>>> GetProductById(int id)
    {
        var product = await productRepository.GetProductByIdAsync(id);
        if (product == null)
        {
            return NotFound();
        }
        return Ok(product);
    }

    [HttpPost]
    public async Task<ActionResult<IEnumerable<Product>>> CreateProducts(Product product)
    {
        productRepository.CreateProduct(product);
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

        if (await productRepository.UpdateProductAsync(id, product))
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
        if (await productRepository.DeleteProductAsync(id))
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