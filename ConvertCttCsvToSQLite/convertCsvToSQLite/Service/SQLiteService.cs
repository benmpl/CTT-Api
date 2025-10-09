using System;
using System.Collections.Generic;
using convertCsvToSQLite.Entity;
using System.Data.SQLite;
using System.Data.Common;
using System.Linq;

namespace convertCsvToSQLite.Service
{
	public class SQLiteService : IDisposable
	{
		public SQLiteConnection Connection { get; set; }

		public SQLiteService()
		{
			if (this.Connection == null || this.Connection.State == System.Data.ConnectionState.Closed)
			{
				string sqlitedb = @"./Database/CTTPortugal.db3";

				SQLiteConnectionStringBuilder connSB = new SQLiteConnectionStringBuilder
				{
					DataSource = sqlitedb,
					FailIfMissing = false,
					Version = 3,
					LegacyFormat = true,
					Pooling = true,
					JournalMode = SQLiteJournalModeEnum.Persist
				};

				SQLiteConnection sqlite = new SQLiteConnection(connSB.ConnectionString);

				try
				{
					sqlite.Open();
				}
				catch (Exception ex)
				{
					throw new Exception("Error openning database", ex);
				}

				this.Connection = sqlite;
			}
		}

		~SQLiteService()
		{
			if (this.Connection.State == System.Data.ConnectionState.Open)
				this.Connection.Close();
		}

		public void GravarDistritos(IList<Distrito> distritos)
		{
			try
			{
				foreach (var distrito in distritos)
				{
					string sql = string.Format("INSERT INTO Distrito (Codigo, Nome) VALUES ('{0}','{1}') ",
						distrito.Codigo.Replace("'", ""),
						distrito.Nome.Replace("'", ""));

					var cmdLite = new SQLiteCommand(sql, this.Connection);

					cmdLite.ExecuteNonQuery();
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error writing Distritos", ex);
			}
		}

		public void GravarConcelhos(IList<Concelho> concelhos)
		{
			try
			{
				foreach (var concelho in concelhos)
				{
					string sql = string.Format("INSERT INTO Concelho (CodigoDistrito, Codigo, Nome) VALUES ('{0}','{1}','{2}') ",
						concelho.CodigoDistrito.Replace("'", ""),
						concelho.Codigo.Replace("'", ""),
						concelho.Nome.Replace("'", ""));

					var cmdLite = new SQLiteCommand(sql, this.Connection);

					cmdLite.ExecuteNonQuery();
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error writing Concelhos", ex);
			}
		}


		public void GravarCodigosPostais(IList<CodigoPostal> codigosPostais)
		{
			try
			{
				var chunks = codigosPostais.Chunk(1000);
				foreach (var chunk in chunks)
				{	
					string sqlStr = "INSERT INTO CodigoPostal (CodigoDistrito, CodigoConcelho, CodigoLocalidade, NomeLocalidade, CodigoArteria, ArteriaTipo, PrimeiraPreposicao, ArteriaTitulo, SegundaPreposicao, ArteriaDesignacao, ArteriaInformacaoLocalZona, Troco, NumeroPorta, NomeCliente, NumeroCodigoPostal, NumeroExtensaoCodigoPostal, DesignacaoPostal) VALUES ";

					foreach (var bit in chunk)
					{
						sqlStr += string.Format("('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', '{14}', '{15}', '{16}'), ",
							bit.Concelho.Distrito.Codigo,
							bit.Concelho.Codigo,
							bit.CodigoLocalidade.Replace("'", ""),
							bit.NomeLocalidade.Replace("'", ""),
							bit.CodigoArteria.Replace("'", ""),
							bit.ArteriaTipo.Replace("'", ""),
							bit.PrimeiraPreposicao.Replace("'", ""),
							bit.ArteriaTitulo.Replace("'", ""),
							bit.SegundaPreposicao.Replace("'", ""),
							bit.ArteriaDesignacao.Replace("'", ""),
							bit.ArteriaInformacaoLocalZona.Replace("'", ""),
							bit.Troco.Replace("'", ""),
							bit.NumeroPorta.Replace("'", ""),
							bit.NomeCliente.Replace("'", ""),
							bit.NumeroCodigoPostal.Replace("'", ""),
							bit.NumeroExtensaoCodigoPostal.Replace("'", ""),
							bit.DesignacaoPostal.Replace("'", ""));
					}
					sqlStr = sqlStr.TrimEnd(' ').TrimEnd(',');
					Console.WriteLine(sqlStr);

					var cmdLite = new SQLiteCommand(sqlStr, this.Connection);

					cmdLite.ExecuteNonQuery();
					
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error writing Códigos Postais", ex);
			}
		}

		public void GravarApartados(IList<Apartado> apartados)
		{
			try
			{
				foreach (var apartado in apartados)
				{
					string sql = string.Format("INSERT INTO Apartado (PostalOfficeIdentification, FirstPOBox, LastPOBox, PostalCode, PostalCodeExtension, PostalName, PostalCodeSpecial, PostalCodeSpecialExtension, PostalNameSpecial) VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}') ",
												apartado.PostalOfficeIdentification.Replace("'", ""),
												apartado.FirstPOBox.Replace("'", ""),
												apartado.LastPOBox.Replace("'", ""),
												apartado.PostalCode.Replace("'", ""),
												apartado.PostalCodeExtension.Replace("'", ""),
												apartado.PostalName.Replace("'", ""),
												apartado.PostalCodeSpecial.Replace("'", ""),
												apartado.PostalCodeSpecialExtension.Replace("'", ""),
												apartado.PostalNameSpecial.Replace("'", ""));

					var cmdLite = new SQLiteCommand(sql, this.Connection);

					cmdLite.ExecuteNonQuery();
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error writing Apartados", ex);
			}
		}

		public void Dispose()
		{
			if (this.Connection != null && this.Connection.State == System.Data.ConnectionState.Open)
				this.Connection.Close();
		}
	}
}
