package krusty;

import spark.Request;
import spark.Response;

import java.sql.*;
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

		try(PreparedStatement statement = conn.prepareStatement(sqlQuery)) {
			ResultSet rs = statement.executeQuery();
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

		try(PreparedStatement statement = conn.prepareStatement(sqlQuery)) {
			ResultSet rs = statement.executeQuery();
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

		try(PreparedStatement statement = conn.prepareStatement(sqlQuery)) {
			ResultSet rs = statement.executeQuery();
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

		try(PreparedStatement statement = conn.prepareStatement(sqlQuery)) {
			ResultSet rs = statement.executeQuery();
			json = Jsonizer.toJson(rs, "recipes");
			return json;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return json;
	}

	public String getPallets(Request req, Response res) {
		String sqlQuery = "SELECT Pallets.pallet_id AS id, Cookies.cookie_name AS cookie, Pallets.production_date, " +
				"Orders.customer_name AS customer, Pallets.is_blocked AS blocked " +
				"FROM Pallets " +
				"LEFT JOIN Cookies " +
				"ON Cookies.cookie_id = Pallets.cookie_id " +
				"LEFT JOIN Orders " +
				"ON Orders.order_id = Pallets.order_id " +
				"ORDER BY production_date DESC;";
		String json = "";
		connect();

		try(PreparedStatement statement = conn.prepareStatement(sqlQuery)) {
			ResultSet rs = statement.executeQuery();
			json = Jsonizer.toJson(rs, "cookies");
			return json;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return json;
	}

	public String reset(Request req, Response res) {
		return "{}";
	}

	public String createPallet(Request req, Response res) {
		return "{}";
	}
}
