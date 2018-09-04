CREATE KEYSPACE finances WITH replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE finances.account_aggregates (
    account_number text,
    average_transaction double,
    total_transactions double,
    number_of_transactions int,
    max_transaction double,
    min_transaction double,
    standard_deviation_amount double,
    unique_transaction_descriptions set<text>,

    PRIMARY KEY(account_number)
);

CREATE TABLE finances.transactions (
    id bigint,
    account_number text,
    amount double,
    date date,
    description text,

    PRIMARY KEY(id)
);