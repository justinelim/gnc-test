-- https://www.youtube.com/watch?v=Ng_zi11N4_c

CREATE DATABASE Transactions_Prod;

USE Transactions_Prod;

CREATE TABLE Transactions (
	transaction_id INT PRIMARY KEY,
    amount DECIMAL (13, 2) NOT NULL,
    transaction_type ENUM('PURCHASE', 'REFUND') NOT NULL
)

DESC Transactions;

INSERT INTO Transactions (transaction_id, amount, transaction_type) VALUES
(111, 13.50, 'PURCHASE'),
(222, 172.99, 'REFUND')

SELECT * FROM Transactions;