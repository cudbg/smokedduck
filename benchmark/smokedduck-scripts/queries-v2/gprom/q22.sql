select main.*, subq.* from (
      SELECT cntrycode,
      count(*) over (partition by cntrycode) AS numcust,
      sum(c_acctbal) over (partition by cntrycode) AS totacctbal,
      customer_rowid_1
      FROM (
        SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal, customer.rowid as customer_rowid_1
        FROM customer
        WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
              AND c_acctbal >  (select avg(c_acctbal) from (
                SELECT c_acctbal FROM customer
                WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
              ))
              AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey=c_custkey)
      )
  ) as main, (select customer.rowid as customer_rowid_0
    FROM customer
      WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
  ) as subq
