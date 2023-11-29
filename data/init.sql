create table if not exists product(
    id int,
    name varchar(100), 
    category varchar(100), 
    price float, 
    last_updated timestamp
    );


INSERT INTO product (id, name, category, price, last_updated)
VALUES
  (1, 'Laptop', 'Electronics', 1200.50, '2023-11-10 12:30:00'),
  (2, 'Coffee Maker', 'Appliances', 99.99, '2023-11-10 14:45:00'),
  (3, 'Running Shoes', 'Fashion', 79.95, '2023-11-10 16:15:00'),
  (4, 'Bookshelf', 'Furniture', 199.99, '2023-11-10 18:00:00'),
  (5, 'Smartphone', 'Electronics', 899.99, '2023-11-11 09:30:00'),
  (6, 'Toaster', 'Appliances', 39.99, '2023-11-11 11:45:00'),
  (7, 'Denim Jeans', 'Fashion', 49.95, '2023-11-11 13:15:00'),
  (8, 'Office Chair', 'Furniture', 149.99, '2023-11-11 15:00:00'),
  (9, 'Headphones', 'Electronics', 149.50, '2023-11-12 09:30:00'),
  (10, 'Blender', 'Appliances', 69.99, '2023-11-12 11:45:00'),
  (11, 'T-shirt', 'Fashion', 19.95, '2023-11-12 13:15:00'),
  (12, 'Desk', 'Furniture', 249.99, '2023-11-12 15:00:00'),
  (13, 'Tablet', 'Electronics', 499.99, '2023-11-13 09:30:00'),
  (14, 'Microwave', 'Appliances', 129.99, '2023-11-13 11:45:00'),
  (15, 'Sneakers', 'Fashion', 59.95, '2023-11-13 13:15:00'),
  (16, 'Sofa', 'Furniture', 799.99, '2023-11-13 15:00:00'),
  (17, 'Digital Camera', 'Electronics', 699.50, '2023-11-14 09:30:00'),
  (18, 'Rice Cooker', 'Appliances', 49.99, '2023-11-14 11:45:00'),
  (19, 'Dress Shirt', 'Fashion', 34.95, '2023-11-14 13:15:00'),
  (20, 'Bed', 'Furniture', 899.99, '2023-11-14 15:00:00');
