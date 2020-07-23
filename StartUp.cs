using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace ExercisesIntroductionDBApps
{
    public static class StartUp
    {
        private static readonly string connectionString = "Server=.;Database=MinionsDB;Integrated Security=True;";
        private static readonly SqlConnection connection = new SqlConnection(connectionString);

        public static void Main()
        {
            connection.Open();

            //VillainNames();
            //MinionNames();
            //AddMinion();
            //ChangeTownNamesCasing();
            //RemoveVillain();
            //PrintAllMinionNames();
            //IncreaseMinionAge();
            IncreaseAgeStoredProcedure();

            connection.Close();
        }

        private static void IncreaseAgeStoredProcedure()
        {
            var minnionId = int.Parse(Console.ReadLine());
            var useProcedureCommand = new SqlCommand("usp_GetOlder", connection)
            {
                CommandType = System.Data.CommandType.StoredProcedure
            };
            useProcedureCommand.Parameters.AddWithValue("@Id", minnionId);
            useProcedureCommand.ExecuteNonQuery();

            var getMinnionCommand = new SqlCommand("SELECT Name, Age FROM Minions WHERE Id = @Id",connection);
            getMinnionCommand.Parameters.AddWithValue("@Id", minnionId);
            var reader = getMinnionCommand.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine($"{reader["Name"]} {reader["Age"]}");
            }
        }

        private static void IncreaseMinionAge()
        {
            var ids = Console.ReadLine()
                .Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .Select(i => int.Parse(i)).ToList();

            foreach (var id in ids)
            {
                var command = new SqlCommand("UPDATE Minions " +
                                             "SET Name = UPPER(LEFT(Name, 1)) + SUBSTRING(Name, 2, LEN(Name)), Age += 1 " +
                                             "WHERE Id = @Id", connection);
                command.Parameters.AddWithValue("@Id", id);
                command.ExecuteNonQuery();
            }

            var getAllMinnionsCommand = new SqlCommand("SELECT Name, Age FROM Minions", connection);
            var reader = getAllMinnionsCommand.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine(reader["Name"]);
            }
        }

        private static void PrintAllMinionNames()
        {
            var getAllMinnionsNamesCommand = new SqlCommand(@"SELECT Name FROM Minions", connection);
            var namesReader = getAllMinnionsNamesCommand.ExecuteReader();

            var names = new List<string>();

            while (namesReader.Read())
            {
                names.Add(namesReader["Name"].ToString());
            }

            var iterations = (int)Math.Round(names.Count / 2.00);
            var countFixNumber = 1;

            for (int i = 0; i < iterations; i++)
            {
                Console.WriteLine(names[i]);

                if (!(names.Count % 2 != 0 && i == iterations - countFixNumber))
                    Console.WriteLine(names[names.Count - (i + countFixNumber)]);
            }
        }

        private static void RemoveVillain()
        {
            var id = int.Parse(Console.ReadLine());

            var getVillainCommand = new SqlCommand(@"SELECT Name FROM Villains WHERE Id = @villainId", connection);
            getVillainCommand.Parameters.AddWithValue("@villainId", id);
            var villainDataReader = getVillainCommand.ExecuteReader();
            
            if (!villainDataReader.HasRows)
            {
                Console.WriteLine("No such villain was found.");
                return;
            }

            string villainName = string.Empty;
            while (villainDataReader.Read())
            {
                villainName = villainDataReader["Name"].ToString();
            }
            villainDataReader.Close();

            var getMinnionsVillainCountCommand = new SqlCommand(@"SELECT COUNT(MinionId) FROM MinionsVillains WHERE VillainId = @villainId", connection);
            getMinnionsVillainCountCommand.Parameters.AddWithValue("@villainId", id);
            var affectedMinnionsCount = getMinnionsVillainCountCommand.ExecuteScalar();

            var transaction = connection.BeginTransaction();

            try
            {
                var deleteCommand = new SqlCommand(@"DELETE FROM MinionsVillains WHERE VillainId = @villainId DELETE FROM Villains WHERE Id = @villainId", connection);
                deleteCommand.Parameters.AddWithValue("@villainId", id);
                deleteCommand.Transaction = transaction;

                deleteCommand.ExecuteNonQuery();
                transaction.Commit();

                Console.WriteLine($"{villainName} was deleted.");
                Console.WriteLine($"{affectedMinnionsCount} minions were released.");
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                Console.WriteLine(ex.Message);
                throw;
            }
            
        }

        private static void ChangeTownNamesCasing()
        {
            var countryName = Console.ReadLine().Trim();

            var townsNameChangeCommandText = @"UPDATE Towns
                                               SET Name = UPPER(Name)
                                               WHERE CountryCode = (SELECT c.Id FROM Countries AS c WHERE c.Name = @countryName)";
            var townsNameChangeCommand = new SqlCommand(townsNameChangeCommandText, connection);
            townsNameChangeCommand.Parameters.AddWithValue("@countryName", countryName);
            townsNameChangeCommand.ExecuteNonQuery();

            var readerCommandText = @"SELECT t.Name 
                                      FROM Towns as t
                                      JOIN Countries AS c ON c.Id = t.CountryCode
                                      WHERE c.Name = @countryName";
            var readerCommand = new SqlCommand(readerCommandText, connection);
            readerCommand.Parameters.AddWithValue("@countryName", countryName);
            var reader = readerCommand.ExecuteReader();
            var townNames = new List<string>();

            if (!reader.HasRows)
            {
                Console.WriteLine("No town names were affected.");
            }
            else
            {
                while (reader.Read())
                {
                    townNames.Add(reader["Name"].ToString());
                }

                Console.WriteLine($"{townNames.Count} town names were affected.");
                Console.WriteLine($"[{string.Join(", ",townNames)}]");
            }
        }

        private static void AddMinion()
        {
            var minionInfomation = Console.ReadLine().Split(' ', StringSplitOptions.RemoveEmptyEntries).Skip(1).ToList();
            var villainName = Console.ReadLine().Split(' ', StringSplitOptions.RemoveEmptyEntries).Skip(1).ToList().FirstOrDefault();

            var minionName = minionInfomation[0];
            var minionAge = int.Parse(minionInfomation[1]);
            var minionTown = minionInfomation[2];

            
            var townCommandText = @"SELECT Id FROM Towns WHERE Name = @townName";
            var command = new SqlCommand(townCommandText, connection);
            command.Parameters.AddWithValue("@townName", minionTown);

            var townId = command.ExecuteScalar();
            if (townId == null)
            {
                var createTownCommand = @"INSERT INTO Towns (Name) VALUES (@townName)";
                var insertTownCommand = new SqlCommand(createTownCommand, connection);
                insertTownCommand.Parameters.AddWithValue("@townName", minionTown);
                insertTownCommand.ExecuteNonQuery();
                townId = command.ExecuteScalar();
                Console.WriteLine($"Town {minionTown} was added to the database.");
            }

            var villinCommandText = @"SELECT Id FROM Villains WHERE Name = @Name";
            var villianCommand = new SqlCommand(villinCommandText, connection);
            villianCommand.Parameters.AddWithValue("@Name", villainName);

            var villianId = villianCommand.ExecuteScalar();
            if (villianId == null)
            {
                var createVillainCommand = @"INSERT INTO Villains (Name, EvilnessFactorId)  VALUES (@villainName, 4)";
                var insertVillainCommand = new SqlCommand(createVillainCommand, connection);
                insertVillainCommand.Parameters.AddWithValue("@villainName", villainName);
                insertVillainCommand.ExecuteNonQuery();
                villianId = villianCommand.ExecuteScalar();
                Console.WriteLine($"Villain {villainName} was added to the database.");
            }

            var createMinnionCommand = @"INSERT INTO Minions (Name, Age, TownId) VALUES (@name, @age, @townId)";
            var inserMinnionCommand = new SqlCommand(createMinnionCommand, connection);
            inserMinnionCommand.Parameters.AddWithValue("@name", minionName);
            inserMinnionCommand.Parameters.AddWithValue("@age", minionAge);
            inserMinnionCommand.Parameters.AddWithValue("@townId", townId);
            inserMinnionCommand.ExecuteNonQuery();

            var getMinnionIdCommand = @"SELECT Id FROM Minions WHERE Name = @Name";
            var cmd = new SqlCommand(getMinnionIdCommand, connection);
            inserMinnionCommand.Parameters.AddWithValue("@name", minionName);
            var minnionId = command.ExecuteScalar();

            var createMappingCommand = @"INSERT INTO MinionsVillains (MinionId, VillainId) VALUES (@villainId, @minionId)";
            var insertMappingCommand = new SqlCommand(createMappingCommand, connection);
            insertMappingCommand.Parameters.AddWithValue("@villainId", villianId);
            insertMappingCommand.Parameters.AddWithValue("@minionId", minnionId);

            Console.WriteLine($"Successfully added {minionName} to be minion of {villainName}.");
        }

        private static void MinionNames()
        {
            var id = int.Parse(Console.ReadLine());

            var transaction = connection.BeginTransaction();

            var command = new SqlCommand();
            command.Transaction = transaction;
            command.Connection = connection;
            command.Parameters.AddWithValue("@Id", id);

            command.CommandText = @"SELECT Name FROM Villains WHERE Id = @Id";
            var reader = command.ExecuteReader();

            if (!reader.HasRows)
            {
                Console.WriteLine($"No villain with ID {id} exists in the database.");
            }
            else
            {
                while (reader.Read())
                {
                    Console.WriteLine($"Villain: {reader["Name"]}");
                }
            }

            command.CommandText = @"SELECT ROW_NUMBER() OVER (ORDER BY m.Name) as RowNum,
                                         m.Name, 
                                         m.Age
                                    FROM MinionsVillains AS mv
                                    JOIN Minions As m ON mv.MinionId = m.Id
                                    WHERE mv.VillainId = @Id
                                    ORDER BY m.Name";

            connection.Close();
            connection.Open();
            reader = command.ExecuteReader();

            using (connection)
            {
                if (!reader.HasRows)
                {
                    Console.WriteLine("(no minions)");
                }
                else
                {
                    while (reader.Read())
                    {
                        Console.WriteLine($"{reader["RowNum"]}. {reader["Name"]} {reader["Age"]}");
                    }
                }
            }
        }

        private static void VillainNames()
        {
            var query = @"SELECT v.Name, COUNT(mv.VillainId) AS MinionsCount  
                              FROM Villains AS v 
                              JOIN MinionsVillains AS mv ON v.Id = mv.VillainId 
                              GROUP BY v.Id, v.Name 
                              HAVING COUNT(mv.VillainId) > 3 
                              ORDER BY COUNT(mv.VillainId)";

            var command = new SqlCommand(query, connection);

            var reader = command.ExecuteReader();

            using (reader)
            {
                while (reader.Read())
                {
                    Console.WriteLine($"{reader["Name"]} - {reader["MinionsCount"]}");
                }
            }
        }
    }
}
