using Calculator.Data;
using Calculator.Services;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllersWithViews();
var connection = builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException("DefaultConnection is required.");
builder.Services.AddDbContext<CalculatorContext>(options =>
    options.UseMySql(connection, new MariaDbServerVersion(new Version(10, 5, 15))));
builder.Services.AddSingleton<KafkaProducerHandler>();
builder.Services.AddSingleton<KafkaProducerService<Null, string>>();
builder.Services.AddHttpClient();
builder.Services.AddHostedService<KafkaConsumerService>();
var app = builder.Build();
if (!app.Environment.IsDevelopment())
    app.UseExceptionHandler("/Calculator/Error");
app.UseStaticFiles();
app.UseRouting();
app.UseAuthorization();
app.MapControllerRoute("default", "{controller=Calculator}/{action=Index}/{id?}");
app.Run();
