using System.IO;
using System.Threading.Tasks;
using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Model.Common;
using Npgsql;
using Xunit;

namespace NpgsqlDockerCompose.Tests;

public class PlainPostgresInsertTests
{
  [Fact]
  public async Task Test1()
  {
    var file = Path.Combine(
      Directory.GetCurrentDirectory(),
      (TemplateString)"Resources/docker-compose.yml"
    );

    var service = new Builder()
      .UseContainer()
      .UseCompose()
      .FromFile(file)
      .RemoveOrphans()
      .ForceRecreate()
      .WaitForPort(
        "database",
        "5432/tcp",
        30000 /*30s*/
      )
      .Build();
    var container = service.Start();

    var connectionString =
      "PORT = 5432; HOST = localhost; TIMEOUT = 15; POOLING = True; COMMANDTIMEOUT = 20; DATABASE = 'postgres'; PASSWORD = '123456'; USER ID = 'postgres'";
    await using var connection = new NpgsqlConnection(connectionString);

    await WaitForPostgres.WaitForConnection(connectionString);

    await connection.OpenAsync();

    var dropTableCommand = new NpgsqlCommand(
      "DROP TABLE IF EXISTS test",
      connection
    );
    await dropTableCommand.ExecuteNonQueryAsync();

    var createTableCommand = new NpgsqlCommand(
      "CREATE TABLE test(id SERIAL PRIMARY KEY, col1 VARCHAR(100) NOT NULL)",
      connection
    );
    await createTableCommand.ExecuteNonQueryAsync();

    await using var cmd = new NpgsqlCommand(
      "INSERT INTO test (col1) VALUES ($1)",
      connection
    )
    {
      Parameters =
      {
        new NpgsqlParameter { Value = "some_value" },
      }
    };

    await cmd.ExecuteNonQueryAsync();

    await using var query = new NpgsqlCommand("SELECT * FROM test", connection);
    var reader = await query.ExecuteReaderAsync();
    Assert.True(reader.HasRows);

    reader.Read();
    Assert.Equal("some_value", reader["col1"].ToString());


    await connection.CloseAsync();
    NpgsqlConnection.ClearAllPools();

    service.Stop();
    service.Remove();
  }

  [Fact]
  public async Task Test2()
  {
    var file = Path.Combine(
      Directory.GetCurrentDirectory(),
      (TemplateString)"Resources/docker-compose.yml"
    );

    var service = new Builder()
      .UseContainer()
      .UseCompose()
      .FromFile(file)
      .RemoveOrphans()
      .ForceRecreate()
      .WaitForPort(
        "database",
        "5432/tcp",
        30000 /*30s*/
      )
      .Build();
    var container = service.Start();

    var connectionString =
      "PORT = 5432; HOST = localhost; TIMEOUT = 15; POOLING = True; COMMANDTIMEOUT = 20; DATABASE = 'postgres'; PASSWORD = '123456'; USER ID = 'postgres'";
    await using var connection = new NpgsqlConnection(connectionString);

    await WaitForPostgres.WaitForConnection(connectionString);
    await connection.OpenAsync();

    var dropTableCommand = new NpgsqlCommand(
      "DROP TABLE IF EXISTS test",
      connection
    );
    await dropTableCommand.ExecuteNonQueryAsync();

    var createTableCommand = new NpgsqlCommand(
      "CREATE TABLE test(id SERIAL PRIMARY KEY, col1 VARCHAR(100) NOT NULL)",
      connection
    );
    await createTableCommand.ExecuteNonQueryAsync();

    await using var cmd = new NpgsqlCommand(
      "INSERT INTO test (col1) VALUES ($1)",
      connection
    )
    {
      Parameters =
      {
        new() { Value = "some_value" },
        // new() { Value = "some_other_value" }
      }
    };

    await cmd.ExecuteNonQueryAsync();

    await using var query = new NpgsqlCommand("SELECT * FROM test", connection);
    var reader = await query.ExecuteReaderAsync();
    Assert.True(reader.HasRows);

    reader.Read();
    Assert.Equal("some_value", reader["col1"].ToString());


    await connection.CloseAsync();
    NpgsqlConnection.ClearAllPools();

    service.Stop();
    service.Remove();
  }
}
