using Microsoft.EntityFrameworkCore;
namespace Calculator.Data
{
    public class CalculatorContext : DbContext
    {
        public DbSet<DataInputVariant> DataInputVariants
        { get; set; }
 public CalculatorContext(DbContextOptions<CalculatorContext> options) : base(options)
        {
        }
        protected override void OnModelCreating(ModelBuilder
       modelBuilder)
        {
            // Preserve the varchar columns and records from Lab 2.
            var entity = modelBuilder.Entity<DataInputVariant>();
            entity.Property(x => x.Operand_1).HasConversion<string>().HasColumnType("varchar(128)");
            entity.Property(x => x.Operand_2).HasConversion<string>().HasColumnType("varchar(128)");
            entity.Property(x => x.Type_operation).HasConversion<string>().HasColumnType("varchar(128)");
        }
    }
}