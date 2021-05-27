package krusty;

import spark.Request;
import spark.Response;

import javax.swing.plaf.nimbus.State;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import static krusty.Jsonizer.toJson;

public class Database {
	/**
	 * Modify it to fit your environment and then use this string when connecting to your database!
	 */

	private static final String jdbcString = "jdbc:mysql://puccini.cs.lth.se";

	// For use with MySQL or PostgreSQL
	private static final String jdbcUsername = "db02";
	private static final String jdbcPassword = "rii142ht";

	public Connection conn = null;

	public void connect() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					jdbcString + "/" + jdbcUsername, jdbcUsername, jdbcPassword
			);
		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	// TODO: Implement and change output in all methods below!

	public String getCustomers(Request req, Response res) {
		String sqlQuery = "SELECT customer_name as name, customer_adress as address FROM customers;";
		String json = "";
		connect();

		try(Statement statement = conn.createStatement()) {
			ResultSet rs = statement.executeQuery(sqlQuery);
			json = Jsonizer.toJson(rs, "customers");
			return json;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return json;
	}

	public String getRawMaterials(Request req, Response res) {
		String sqlQuery = "SELECT ingredient_name as name, in_stock as amount, measure as unit FROM ingredients;";
		String json = "";
		connect();

		try(Statement statement = conn.createStatement()) {
			ResultSet rs = statement.executeQuery(sqlQuery);
			json = Jsonizer.toJson(rs, "raw-materials");
			return json;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return json;
	}

	public String getCookies(Request req, Response res) {
		String sqlQuery = "SELECT cookie_name AS name FROM Cookies;";
		String json = "";
		connect();

		try(Statement statement = conn.createStatement()) {
			ResultSet rs = statement.executeQuery(sqlQuery);
			json = Jsonizer.toJson(rs, "cookies");
			return json;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return json;
	}

	public String getRecipes(Request req, Response res) {
		String sqlQuery = "SELECT Cookies.cookie_name AS cookie, Ingredients.ingredient_name AS raw_material, RecipeItems.quantity AS amount, Ingredients.measure AS unit " +
				"FROM RecipeItems " +
				"LEFT JOIN Ingredients " +
				"ON RecipeItems.ingredient_id = Ingredients.ingredient_id" +
				"LEFT JOIN Cookies" +
				"ON Cookies.cookie_id = RecipeItems.cookie_id" +
				"ORDER BY cookie ASC;";
		String json = "";
		connect();

		try(Statement statement = conn.createStatement()) {
			ResultSet rs = statement.executeQuery(sqlQuery);
			json = Jsonizer.toJson(rs, "recipes");
			return json;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return json;
	}

	public String getPallets(Request req, Response res) {

		String sqlQuery = "SELECT Pallets.pallet_id AS id, Cookies.cookie_name AS cookie, Pallets.production_date, " +
				"Orders.customer_name AS customer, Pallets.is_blocked, IF(is_blocked, 'yes', 'no') AS blocked " +
				"FROM Pallets " +
				"LEFT JOIN Cookies " +
				"ON Cookies.cookie_id = Pallets.cookie_id " +
				"LEFT JOIN Orders " +
				"ON Orders.order_id = Pallets.order_id ";

		ArrayList<String> filterValues = new ArrayList<String>();
		StringBuilder sb = new StringBuilder(sqlQuery);
		int querySize = sqlQuery.length();

		if (req.queryParams("from") != null) {
			if(querySize == sb.toString().length()) {
				sb.append("WHERE production_date >= ? ");
			} else {
				sb.append("AND production_date >= ? ");
			}
			filterValues.add(req.queryParams("from"));
		}

		if (req.queryParams("to") != null) {
			if(querySize == sb.toString().length()) {
				sb.append("WHERE production_date <= ? ");
			} else {
				sb.append("AND production_date <= ? ");
			}
			filterValues.add(req.queryParams("to"));
		}

		if (req.queryParams("cookie") != null) {
			if(querySize == sb.toString().length()) {
				sb.append("WHERE cookie_name = ? ");
			} else {
				sb.append("AND cookie_name = ? ");
			}
			filterValues.add(req.queryParams("cookie"));
		}

		System.out.println(req.queryParams("blocked"));

		if (req.queryParams("blocked") != null) {
			if(querySize == sb.toString().length()) {
				sb.append("WHERE is_blocked = ? ");
			} else {
				sb.append("AND is_blocked = ? ");
			}

			if(req.queryParams("blocked").toLowerCase().equals("yes")) {
				filterValues.add("true");
			} else {
				filterValues.add("false");
			}

		}

		String json = "";
		connect();
		sb.append("ORDER BY production_date DESC;");
		sqlQuery = sb.toString();

		try(PreparedStatement statement = conn.prepareStatement(sqlQuery)) {
			for (int i = 0; i < filterValues.size(); i++) {
				System.out.println("Filtervalue" + i + ": " + filterValues.get(i));
				if(filterValues.get(i).equals("false") ){
					statement.setBoolean(i+1, false);
				} else if(filterValues.get(i).equals("true")) {
					statement.setBoolean(i+1, true);
				}else {
					statement.setString(i+1, filterValues.get(i));
				}

			}
			System.out.println( "Query: " + sqlQuery);
			System.out.println( "Statement: " + statement.toString());
			ResultSet rs = statement.executeQuery();

			json = Jsonizer.toJson(rs, "pallets");
			System.out.println("json string: " + json);
			return json;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return json;
	}

	public String reset(Request req, Response res) {

		String sqlTruncateRecipeItems = "TRUNCATE TABLE RecipeItems;";
		String sqlTruncateOrderSpecs = "TRUNCATE TABLE OrderSpecs;";
		String sqlTruncatePallets = "TRUNCATE TABLE Pallets;";
		String sqlTruncateCookies = "TRUNCATE TABLE Cookies;";
		String sqlTruncateOrders = "TRUNCATE TABLE Orders;";
		String sqlTruncateIngredients = "TRUNCATE TABLE Ingredients;";
		String sqlTruncateCustomers = "TRUNCATE TABLE Customers;";

		String sqlFKCheckZero = "SET FOREIGN_KEY_CHECKS = 0;";
		String sqlFKCheckOne = "SET FOREIGN_KEY_CHECKS = 1;";

		String sqlInsertCustomers = "INSERT INTO Customers (\n" +
				"customer_name, customer_adress\n" +
				")  \n" +
				"VALUES\n" +
				"(\"Finkakor AB\", \"Helsingborg\"), \n" +
				"(\"Småbröd AB\", \"Malmö\"),\n" +
				"(\"Kaffebröd AB\", \"Landskrona\"),\n" +
				"(\"Bjudkakor AB\", \"Ystad\"),\n" +
				"(\"Kalaskakor AB\", \"Trelleborg\"),\n" +
				"(\"Partykakor AB\", \"Kristianstad\"),\n" +
				"(\"Gästkakor AB\", \"Hässleholm\"),\n" +
				"(\"Skånekakor AB\", \"Perstorp\");";

		String sqlInsertCookies = "INSERT INTO Cookies \n" +
				"(cookie_name)\n" +
				"VALUES\n" +
				"(\"Nut ring\"),\n" +
				"(\"Nut cookie\"),\n" +
				"(\"Amneris\"),\n" +
				"(\"Tango\"),\n" +
				"(\"Almond delight\"),\n" +
				"(\"Berliner\");";

		String sqlInsertIngredients = "INSERT INTO Ingredients\n" +
				"(ingredient_name, in_stock, measure)\n" +
				"VALUES\n" +
				"(\"Flour\", 500000, \"g\"), \n" +
				"(\"Butter\", 500000, \"g\"), \n" +
				"(\"Icing sugar\", 500000, \"g\"), \n" +
				"(\"Roasted, chopped nuts\", 500000, \"g\"), \n" +
				"(\"Fine-ground nuts\", 500000, \"g\"), \n" +
				"(\"Bread crumbs\", 500000, \"g\"), \n" +
				"(\"Sugar\", 500000, \"g\"), \n" +
				"(\"Egg whites\", 500000, \"ml\"), \n" +
				"(\"Chocolate\", 500000, \"g\"), \n" +
				"(\"Marzipan\", 500000, \"g\"), \n" +
				"(\"Eggs\", 500000, \"g\"), \n" +
				"(\"Potato starch\", 500000, \"g\"), \n" +
				"(\"Wheat flour\", 500000, \"g\"), \n" +
				"(\"Sodium bicarbonate\", 500000, \"g\"),\n" +
				"(\"Vanilla\", 500000, \"g\"), \n" +
				"(\"Chopped almonds\", 500000, \"g\"), \n" +
				"(\"Cinnamon\", 500000, \"g\"), \n" +
				"(\"Vanilla sugar\", 500000, \"g\"), \n" +
				"(\"Ground, roasted nuts\", 500000, \"g\");";

		String sqlInsertRecipeItems = "INSERT INTO RecipeItems \n" +
				"(ingredient_id, cookie_id, quantity) \n" +
				"VALUES \n" +
				"(1, 1, 450), \n" +
				"(2, 1, 450), \n" +
				"(3, 1, 190), \n" +
				"(4, 1, 225), \n" +
				"(5, 2, 750), \n" +
				"(6, 2, 125), \n" +
				"(7, 2, 375), \n" +
				"(8, 2, 3.5), \n" +
				"(9, 2, 50), \n" +
				"(10, 3, 750), \n" +
				"(2, 3, 250), \n" +
				"(11, 3, 250), \n" +
				"(12, 3, 25), \n" +
				"(13, 3, 25), \n" +
				"(2, 4, 200), \n" +
				"(7, 4, 250), \n" +
				"(1, 4, 300), \n" +
				"(14, 4, 4), \n" +
				"(15, 4, 2), \n" +
				"(2, 5, 400),\n" +
				"(7, 5, 270), \n" +
				"(16, 5, 279), \n" +
				"(1, 5, 400), \n" +
				"(17, 5, 10), \n" +
				"(1, 6, 350), \n" +
				"(2, 6, 250), \n" +
				"(3, 6, 100), \n" +
				"(11, 6, 50), \n" +
				"(18, 6, 5), \n" +
				"(9, 6, 50);";

		connect();

		try(Statement truncateRecipeItems = conn.createStatement();
			Statement truncateOrderSpecs = conn.createStatement();
			Statement truncatePallets = conn.createStatement();
			Statement truncateCookies = conn.createStatement();
			Statement truncateOrders = conn.createStatement();
			Statement truncateIngredients = conn.createStatement();
			Statement truncateCustomers = conn.createStatement();
			PreparedStatement insertCustomerStatement = conn.prepareStatement(sqlInsertCustomers);
			PreparedStatement insertCookiesStatement = conn.prepareStatement(sqlInsertCookies);
			PreparedStatement insertIngredientsStatement = conn.prepareStatement(sqlInsertIngredients);
			PreparedStatement insertRecipeItems = conn.prepareStatement(sqlInsertRecipeItems);
			Statement disableFKCheck = conn.createStatement();
			Statement enableFKCheck = conn.createStatement()) {

			disableFKCheck.executeUpdate(sqlFKCheckZero);
			truncateRecipeItems.executeUpdate(sqlTruncateRecipeItems);
			truncateOrderSpecs.executeUpdate(sqlTruncateOrderSpecs);
			truncatePallets.executeUpdate(sqlTruncatePallets);
			truncateCookies.executeUpdate(sqlTruncateCookies);
			truncateOrders.executeUpdate(sqlTruncateOrders);
			truncateIngredients.executeUpdate(sqlTruncateIngredients);
			truncateCustomers.executeUpdate(sqlTruncateCustomers);
			enableFKCheck.executeUpdate(sqlFKCheckOne);

			insertCustomerStatement.executeUpdate();
			insertCookiesStatement.executeUpdate();
			insertIngredientsStatement.executeUpdate();
			insertRecipeItems.executeUpdate();

		} catch(SQLException e) {
			e.printStackTrace();
		}

		String json = "{ \n" +
				"  \"status\": \"ok\" \n" +
				"}";
		return json;
	}

	public String createPallet(Request req, Response res) {
		String sqlQuery = "SELECT cookie_name AS name, cookie_id AS id FROM Cookies WHERE cookie_name = ?;";
		String sqlGetRecipeItems = "SELECT ingredient_id, quantity FROM RecipeItems WHERE cookie_id = ?;";
		String sqlUpdateInStock = "UPDATE Ingredients SET in_stock = ((SELECT in_stock as quantity FROM (SELECT * FROM Ingredients) AS x " +
				"WHERE ingredient_id = ?) - (54 * ?)) WHERE ingredient_id = ?;";
		String json = "";
		int cookieID = 0;
		boolean cookieExist = false;
		int id = 0;
		connect();
		/*Försök att hitta kakan som vi försöker skapa en pall av */
		try(PreparedStatement statement = conn.prepareStatement(sqlQuery)) {
			statement.setString(1, req.queryParams("cookie"));
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				cookieID = rs.getInt("id");
				cookieExist = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		/*om kaka finns så skapar vi en pall */
		if(cookieExist) {
			sqlQuery = "INSERT INTO Pallets (production_date, cookie_id) " +
					"VALUES (NOW(), (SELECT cookie_id FROM Cookies WHERE cookie_name = ?));";

			try(PreparedStatement statement = conn.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS);
				PreparedStatement statement2 = conn.prepareStatement(sqlGetRecipeItems);
				PreparedStatement statement3 = conn.prepareStatement(sqlUpdateInStock)) {

				statement.setString(1, req.queryParams("cookie"));
				statement2.setInt(1, cookieID);
				statement.executeUpdate();
				ResultSet rs2 = statement.getGeneratedKeys();
				ResultSet rsRecipeItems = statement2.executeQuery();

				if (rs2.next()) {
					id = rs2.getInt(1);
				}

				while(rsRecipeItems.next()) {
					statement3.setInt(1, rsRecipeItems.getInt("ingredient_id"));
					statement3.setInt(2, rsRecipeItems.getInt("quantity"));
					statement3.setInt(3, rsRecipeItems.getInt("ingredient_id"));
					statement3.executeUpdate();
				}

				json = "{\n\t\"status\": \"ok\",\n\t\"id\": " + id + "\n }";
			} catch (SQLException e) {
				json = "{\n\t\"status\": \"error\"\n}";
				e.printStackTrace();
			}
		} else {
			json = "{\n\t\"status\": \"unknown cookie\"\n}"; //kakan fanns inte, returnera det
		}
		return json;
	}
}
