package shop;

public class Product {
	String name;
	double quantity;
	double price;
	double profit;
	
	public Product(String name, double quantity, double price, double profit) {
		this.name = name;
		this.quantity = quantity;
		this.price = price;
		this.profit = profit;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getQuantity() {
		return quantity;
	}

	public void setQuantity(double quantity) {
		this.quantity = quantity;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public double getProfit() {
		return profit;
	}

	public void setProfit(double profit) {
		this.profit = profit;
	}
	
	public void incrementQuantity(double quantity) {
		this.setQuantity(this.getQuantity() + quantity);
	}
	
	public void incrementPrice(double price) {
		this.setPrice(this.getPrice() + price);
	}
	
	public void incrementProfit(double profit) {
		this.setProfit(this.getProfit() + profit);
	}
	
	public void incrementAll(Product product) {
		this.incrementPrice(product.getPrice());
		this.incrementQuantity(product.getQuantity());
		this.incrementProfit(product.getProfit());
	}

}
