using System;
using System.Threading.Tasks;
using Npgsql;

namespace NpgsqlDockerCompose.Tests;

public class WaitForPostgres
{
  public static async Task WaitForConnection(
    string connectionString
  )
  {
    var connection = new NpgsqlConnection(connectionString);
    try
    {
      connection.Open();
      var command = new NpgsqlCommand(
        "SELECT 1",
        connection
      );
      await command.ExecuteNonQueryAsync();
    }
    catch (NpgsqlException e)
    {
      await connection.CloseAsync();
      await Task.Delay(500);
      await WaitForConnection(connectionString);
    }
  }
}
