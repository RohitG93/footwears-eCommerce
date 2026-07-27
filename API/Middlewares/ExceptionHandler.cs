
using System.Net;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using API.RequestHelpers;

namespace API.Middlewares;

public class ExceptionHandler(IHostEnvironment env, RequestDelegate next)
{
    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await next(context);
        }
        catch(Exception ex)
        {
            await HandleException(context, ex, env);
        }
    }

    private static Task HandleException(HttpContext context, Exception ex, IHostEnvironment env)
    {
        context.Response.ContentType = "application/json";
        context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;

        var response = env.IsDevelopment()
            ? new ExceptionError(context.Response.StatusCode, ex.Message, ex.StackTrace)
            : new ExceptionError(context.Response.StatusCode, ex.Message, "Internal Server error");


        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

        var json = JsonSerializer.Serialize(response, options);

        return context.Response.WriteAsync(json);
    }
}